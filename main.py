import os
import pandas as pd
import re
import unicodedata
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    filters,
    ContextTypes,
    CommandHandler,
    ConversationHandler,
    PicklePersistence, # Importar PicklePersistence
)
import logging
import functools # Para el decorador

# --- Configuración ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuración Transcriptor (mantén tu elección)
try:
    from GoogleTranscriptor import GoogleTranscriptor
    transcriptor = GoogleTranscriptor()
    logger.info("Usando GoogleTranscriptor")
except ImportError:
    logger.warning("GoogleTranscriptor no encontrado. La transcripción de audio fallará.")
    transcriptor = None # O poner un transcriptor dummy

# Directorio para guardar los datos de usuario (archivos Excel)
USER_DATA_DIR = "user_data"
os.makedirs(USER_DATA_DIR, exist_ok=True)

# Nombre del archivo para guardar el estado del bot (sesiones, etc.)
PERSISTENCE_FILE = "bot_persistence"

# Estados de la conversación de autenticación
ASKING_KEY, AUTHENTICATED = range(2)
# Estados para registro de gastos por texto con /registrargasto
ESPERANDO_GASTOS_COMANDO = 3 # Renombrado para claridad, aunque el valor no cambia
# Estados para eliminar gasto específico
LISTANDO_GASTOS_ELIMINAR, ESPERANDO_NUMERO_ELIMINAR = range(10, 12)

# --- Funciones Auxiliares ---

def sanitize_key(key: str) -> str:
    """Limpia una clave para usarla como nombre de archivo."""
    key = key.lower().strip()
    key = unicodedata.normalize('NFKD', key).encode('ASCII', 'ignore').decode('utf-8')
    key = re.sub(r'\W+', '_', key)
    key = re.sub(r'_+', '_', key)
    key = key.strip('_')
    return key if key else "invalid_key"

def get_user_file_path(context: ContextTypes.DEFAULT_TYPE) -> str | None:
    """Obtiene la ruta al archivo Excel del usuario autenticado."""
    user_key = context.user_data.get('user_key_sanitized')
    if user_key:
        return os.path.join(USER_DATA_DIR, f"{user_key}.xlsx")
    return None

def require_authentication(func):
    """Decorador para asegurar que el usuario esté autenticado."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        # Si el mensaje es None (p.ej., callback query), no procesar aquí
        if not update.message:
             logger.debug("require_authentication: update.message is None, skipping check.")
             # Permitir que otros handlers (como los de ConversationHandler) lo manejen
             # O decidir qué hacer en este caso. Por ahora, lo dejamos pasar.
             return await func(update, context, *args, **kwargs)

        user_file = get_user_file_path(context)
        if not user_file:
            # Verificar si el mensaje es /start o /cancel para permitir el inicio de sesión o cancelación
            if update.message.text and update.message.text.startswith(('/start', '/cancel')):
                 logger.debug(f"require_authentication: Allowing {update.message.text} for unauthenticated user.")
                 return await func(update, context, *args, **kwargs)

            await update.message.reply_text(
                "🔐 Necesitas iniciar sesión primero. Usa /start para ingresar tu clave."
            )
            # Salir si está en una conversación que requiere auth
            current_state = None
            # Comprobar si hay un ConversationHandler activo asociado a esta actualización
            # Nota: application.conversation_handler no es la forma estándar,
            # la comprobación del estado suele hacerse dentro del propio handler o con check_update
            # Pero si estamos fuera de un flujo de conversación definido, simplemente retornamos.
            # Si el decorador se aplica a una función *fuera* de un ConversationHandler,
            # no necesitamos devolver ConversationHandler.END. Simplemente retornamos None.
            # Si se aplica a una función *dentro* de un ConversationHandler,
            # la lógica del handler (o sus fallbacks) debería manejar la salida.
            # Por simplicidad aquí, solo retornamos None si no está autenticado y no es /start o /cancel.
            return None # Indica que la función decorada no debe ejecutarse

        return await func(update, context, *args, **kwargs)
    return wrapper


def normalizar_texto(texto):
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto

# --- Lógica de Procesamiento y Guardado ---

def procesar_texto_audio(texto, usuario):
    """Procesa texto proveniente de AUDIO (busca 'compre' o 'gaste')."""
    texto_normalizado = normalizar_texto(texto)
    transacciones = []
    patron = r'\b(compre|gaste)\b[^\d]*([\d\.,]+)(.*?)(?=\b(?:compre|gaste)\b|$)'
    matches = re.finditer(patron, texto_normalizado, re.DOTALL | re.IGNORECASE)

    for match in matches:
        tipo = "compra" if match.group(1).lower() == "compre" else "gasto"
        cantidad_str = match.group(2).strip().replace('.', '').replace(',', '.')
        descripcion = match.group(3).strip()
        descripcion = re.sub(r'^\W+', '', descripcion)
        descripcion = re.sub(r'\s+', ' ', descripcion).capitalize() or "Sin descripción"

        try:
            cantidad = float(cantidad_str)
            transacciones.append({
                "Tipo": tipo, "Monto": cantidad, "Descripción": descripcion,
                "Usuario": usuario, "Fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        except ValueError:
            logger.warning(f"No se pudo convertir monto (audio): {match.group(2)} en texto: {texto}")
    return transacciones

def _parsear_fecha_texto(fecha_str: str) -> datetime:
    """Función auxiliar para parsear fechas de texto (hoy, ayer, dd/mm, etc.)."""
    hoy = datetime.now()
    fecha_str = fecha_str.strip().lower()
    if fecha_str == 'ayer': return (hoy - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
    if fecha_str == 'hoy': return hoy.replace(hour=12, minute=0, second=0, microsecond=0)
    for fmt in ["%d/%m", "%d-%m"]:
        try:
            parsed_date = datetime.strptime(fecha_str, fmt)
            # Asume año actual si no se especifica
            year_to_use = hoy.year
            # Si la fecha resultante es futura (p.ej., hoy es Ene, fecha es Dic), asume año anterior
            temp_date = parsed_date.replace(year=year_to_use, hour=12, minute=0, second=0, microsecond=0)
            if temp_date > hoy + timedelta(days=1): # Margen pequeño para evitar problemas de zona horaria
                 year_to_use -= 1
            return parsed_date.replace(year=year_to_use, hour=12, minute=0, second=0, microsecond=0)
        except ValueError: continue
    for fmt in ["%d/%m/%y", "%d-%m-%y", "%d/%m/%Y", "%d-%m-%Y"]:
        try:
            dt = datetime.strptime(fecha_str, fmt)
            # Corregir año si es yy y potencialmente ambiguo (p.ej. '24' podría ser 1924 o 2024)
            # Asumimos que años < 70 son del siglo 21
            year = dt.year
            if len(fecha_str.split('/')[-1]) == 2 or len(fecha_str.split('-')[-1]) == 2:
                if year < 70: year += 2000 # Asume 20xx
                elif year < 100: year += 1900 # Asume 19xx (menos probable para gastos)
            # No permitir fechas muy futuras
            final_date = dt.replace(year=year, hour=12, minute=0, second=0, microsecond=0)
            if final_date > hoy + timedelta(days=3): # Permitir un par de días en el futuro
                raise ValueError(f"Fecha futura no permitida: {fecha_str}")
            return final_date
        except ValueError: continue
    raise ValueError(f"Formato fecha no soportado: '{fecha_str}'")


async def _procesar_y_guardar_gasto_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Función REUTILIZABLE para procesar texto de gastos (formato 'MONTO en DESC (FECHA)')
    y guardarlos en el Excel del usuario.
    Llamada tanto por el handler de /registrargasto como por el handler de texto genérico.
    """
    user_file_path = get_user_file_path(context)
    if not user_file_path:
        # Esto no debería pasar si @require_authentication funciona, pero por si acaso.
        await update.message.reply_text("❌ Error interno: No se encontró tu archivo de usuario. Intenta /start de nuevo.")
        return

    user = update.message.from_user
    nombre_usuario = user.first_name or user.username or f"User_{user.id}"
    texto_completo = update.message.text

    transacciones_procesadas = []
    errores = []
    # Mejoramos el regex para ser más flexible con los espacios y permitir puntos/comas en montos
    # El patrón busca: inicio, opcionalmente espacios, digitos/punto/coma, espacios, 'en', espacios, descripción, opcionalmente (espacios, '(', fecha, ')'), opcionalmente espacios, fin.
    patron = r'^\s*([\d.,]+)\s+(?:en|de|para)\s+(.+?)(?:\s+\((.+)\))?\s*$'
    # Dividir por nueva línea o punto, filtrando líneas vacías
    lineas = [linea.strip() for linea in re.split(r'[.\n]+', texto_completo) if linea.strip()]

    if not lineas:
        # Si el texto está vacío o solo contiene separadores, no hagas nada o informa.
        # Podríamos decidir ignorar estos mensajes silenciosamente o responder.
        # Por ahora, responderemos que no se detectó formato.
        await update.message.reply_text("🤔 No detecté ningún gasto en el formato esperado (`MONTO en DESCRIPCION (FECHA)`).")
        return

    for i, linea_limpia in enumerate(lineas, 1):
        match = re.match(patron, linea_limpia, re.IGNORECASE)
        if not match:
            # Si no coincide con el patrón principal, podría ser un mensaje normal
            # PERO si el usuario envió específicamente texto a este bot,
            # es probable que intentara registrar un gasto. Informar del error.
            errores.append(f"Línea {i}: Formato incorrecto -> '{linea_limpia}'")
            continue

        monto_str, descripcion, fecha_str_raw = match.groups()
        fecha_str = fecha_str_raw or 'hoy' # Fecha por defecto es 'hoy'

        try:
            # Limpiar monto: quitar puntos de miles, usar coma como decimal si existe
            if ',' in monto_str and '.' in monto_str: # Ej: 1.234,56
                monto_str_limpio = monto_str.replace('.', '').replace(',', '.')
            elif ',' in monto_str: # Ej: 1234,56
                monto_str_limpio = monto_str.replace(',', '.')
            else: # Ej: 1234.56 o 1234
                monto_str_limpio = monto_str # Asume punto como decimal si existe
            monto = float(monto_str_limpio)
            if monto <= 0: raise ValueError("Monto debe ser positivo")
        except ValueError as e:
            errores.append(f"Línea {i}: Monto inválido '{monto_str}' -> {e}")
            continue

        try:
            fecha = _parsear_fecha_texto(fecha_str.strip())
        except ValueError as e:
            errores.append(f"Línea {i}: Fecha inválida '{fecha_str}' -> {e}")
            continue

        descripcion_limpia = descripcion.strip().capitalize()
        transacciones_procesadas.append({
            "Tipo": "gasto", "Monto": monto, "Descripción": descripcion_limpia,
            "Usuario": nombre_usuario, "Fecha": fecha.strftime("%Y-%m-%d %H:%M:%S")
        })

    respuesta = ""
    if transacciones_procesadas:
        if guardar_en_excel(transacciones_procesadas, user_file_path):
            respuesta += "✅ Gastos registrados:\n" + "\n".join(
                [f"- ${t['Monto']:,.2f} en {t['Descripción']} ({datetime.strptime(t['Fecha'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')})"
                 for t in transacciones_procesadas])
        else:
            respuesta += "⚠️ Falló el guardado en Excel.\n"
    # Solo mostrar que no se encontraron gastos si NO hubo errores Y no se procesó nada
    elif not errores:
         respuesta += "ℹ️ No se encontraron gastos con el formato correcto en tu mensaje.\n"

    if errores:
        respuesta += "\n\n❌ Errores encontrados:\n" + "\n".join(errores)
        respuesta += "\n\nRevisa el formato: `MONTO en DESCRIPCION (FECHA)` donde la fecha es opcional (ayer, hoy, dd/mm, dd/mm/yy)."

    # Evitar enviar un mensaje vacío si no hubo ni transacciones ni errores (poco probable pero posible)
    if respuesta:
        await update.message.reply_text(respuesta, parse_mode="Markdown")
    else:
        logger.info(f"Mensaje de {nombre_usuario} no generó respuesta (sin transacciones ni errores detectados): '{texto_completo}'")


def guardar_en_excel(transacciones, excel_file_path):
    """Guarda las transacciones en el archivo Excel específico del usuario."""
    if not excel_file_path:
        logger.error("Intento de guardar sin ruta de archivo válida.")
        return False
    if not transacciones:
        logger.info("No hay transacciones para guardar.")
        return False # No es un error, pero no se hizo nada

    try:
        df_nuevo = pd.DataFrame(transacciones)
        column_order = ['Fecha', 'Usuario', 'Tipo', 'Monto', 'Descripción']
        # Asegurarse de que las columnas existen antes de reordenar
        for col in column_order:
            if col not in df_nuevo.columns:
                df_nuevo[col] = pd.NA # O un valor por defecto apropiado
        df_nuevo = df_nuevo[column_order]

        os.makedirs(os.path.dirname(excel_file_path), exist_ok=True)
        df_final = df_nuevo

        if os.path.exists(excel_file_path):
            try:
                # Intentar leer con manejo específico de archivo vacío
                try:
                    df_existente = pd.read_excel(excel_file_path)
                except ValueError as ve: # A veces Pandas lanza ValueError en archivos corruptos/vacíos
                    if "Excel file format cannot be determined" in str(ve) or "File is empty" in str(ve):
                        logger.warning(f"Archivo Excel {excel_file_path} vacío o corrupto. Se sobrescribirá.")
                        df_existente = pd.DataFrame(columns=column_order)
                    else: raise # Re-lanzar otro ValueError
                except Exception as read_err: # Capturar otros errores de lectura
                    logger.error(f"Error al leer Excel {excel_file_path}: {read_err}. Se intentará sobrescribir.")
                    df_existente = pd.DataFrame(columns=column_order) # Crear DF vacío para intentar sobrescribir

                if not df_existente.empty:
                     # Verificar columnas antes de concatenar
                     if all(col in df_existente.columns for col in column_order):
                         # Asegurar tipos consistentes antes de concatenar si es posible
                         try:
                             df_existente['Fecha'] = pd.to_datetime(df_existente['Fecha'], errors='coerce')
                             df_existente['Monto'] = pd.to_numeric(df_existente['Monto'], errors='coerce')
                         except Exception as type_err:
                             logger.warning(f"Error al convertir tipos en archivo existente {excel_file_path}: {type_err}. Concatenación podría fallar.")

                         # Asegurarse que las columnas a concatenar existen en ambos DFs
                         cols_existentes = df_existente.columns.intersection(df_nuevo.columns)
                         df_final = pd.concat([df_existente[cols_existentes], df_nuevo[cols_existentes]], ignore_index=True)
                         # Rellenar columnas faltantes si las hubiera después de concatenar
                         for col in column_order:
                             if col not in df_final.columns: df_final[col] = pd.NA
                         df_final = df_final[column_order] # Reordenar finalmente
                     else:
                         logger.warning(f"Excel {excel_file_path} con formato de columnas inesperado. Se intentará concatenar de todas formas o sobrescribir.")
                         # Intento básico de concatenar, puede fallar si las columnas son muy diferentes
                         try:
                              df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
                              df_final = df_final[column_order] # Intentar reordenar
                         except Exception as concat_err:
                              logger.error(f"Fallo al concatenar DFs con columnas diferentes: {concat_err}. Se sobrescribirá el archivo.")
                              df_final = df_nuevo # Volver a usar solo el nuevo DF como último recurso

            except Exception as read_err:
                logger.error(f"Error GENERAL al leer/procesar Excel existente {excel_file_path}: {read_err}. Se intentará sobrescribir.")
                df_final = df_nuevo # Sobrescribir si la lectura falla catastróficamente

        # Asegurar tipos antes de guardar
        df_final['Fecha'] = pd.to_datetime(df_final['Fecha'], errors='coerce')
        df_final['Monto'] = pd.to_numeric(df_final['Monto'], errors='coerce')
        df_final.sort_values(by='Fecha', inplace=True, na_position='first') # Ordenar por fecha

        df_final.to_excel(excel_file_path, index=False, engine='openpyxl') # Especificar engine puede ayudar
        logger.info(f"Transacciones guardadas en {excel_file_path}")
        return True
    except Exception as e:
        logger.exception(f"Error CRÍTICO al guardar en Excel {excel_file_path}") # Usar exception para stack trace
        return False

# --- Handlers de Comandos (Audio, Descarga, Eliminar Último) ---

@require_authentication
async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    if not transcriptor:
         await update.message.reply_text("❌ Transcripción de audio no configurada.")
         return
    if not user_file_path:
         await update.message.reply_text("❌ Error interno: archivo de usuario no encontrado.")
         return

    audio_temp_path = "audio_temp_file"
    user = update.message.from_user
    nombre_usuario = user.first_name or user.username or f"User_{user.id}"
    audio_file = update.message.audio or update.message.voice
    if not audio_file:
        logger.warning("handle_audio llamado sin archivo de audio.")
        return

    try:
        new_file = await context.bot.get_file(audio_file.file_id)
        file_extension = audio_file.mime_type.split('/')[-1] if audio_file.mime_type else 'oga'
        # Usar un nombre temporal único si es posible, o manejar concurrencia
        audio_temp_path_with_ext = f"{audio_temp_path}_{user.id}_{datetime.now().timestamp()}.{file_extension}"
        await new_file.download_to_drive(audio_temp_path_with_ext)

        texto_transcrito = await transcriptor.transcribir(audio_temp_path_with_ext)
        logger.info(f"Texto transcrito de audio para {user.id}: {texto_transcrito}")
        # Usar la función específica para audio
        transacciones = procesar_texto_audio(texto_transcrito, nombre_usuario)

        respuesta = f"🎤 *Usuario*: {nombre_usuario}\n📝 *Transcripción*:\n\n`{texto_transcrito}`\n\n"
        if transacciones:
            if guardar_en_excel(transacciones, user_file_path):
                respuesta += "✅ *Transacciones (audio) registradas:*\n" + "\n".join(
                    [f"- {t['Tipo'].capitalize()}: ${t['Monto']:,.2f} - {t['Descripción']}" for t in transacciones])
            else:
                respuesta += "⚠️ Falló el guardado en Excel."
        else:
            respuesta += "ℹ️ No se detectaron transacciones tipo 'compre'/'gaste' en el audio."

        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except Exception as e:
        logger.exception(f"Error procesando audio para {user.id}")
        await update.message.reply_text(f"❌ Error procesando audio: {str(e)}")
    finally:
        # Limpiar archivo temporal
        if 'audio_temp_path_with_ext' in locals() and os.path.exists(audio_temp_path_with_ext):
            try:
                os.remove(audio_temp_path_with_ext)
                logger.debug(f"Archivo temporal de audio eliminado: {audio_temp_path_with_ext}")
            except OSError as rm_err:
                logger.error(f"Error eliminando archivo temporal de audio {audio_temp_path_with_ext}: {rm_err}")
        else:
            # Limpieza genérica por si el nombre falló (menos seguro)
            possible_paths = [p for p in os.listdir('.') if p.startswith(audio_temp_path)]
            for p in possible_paths:
                try: os.remove(p)
                except OSError as rm_err: logger.error(f"Error eliminando {p} genéricamente: {rm_err}")


@require_authentication
async def eliminar_operacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Elimina la *última* operación registrada."""
    user_file_path = get_user_file_path(context)
    if not user_file_path: return await update.message.reply_text("❌ Error interno: archivo no encontrado.")
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("❌ No hay operaciones registradas.")
        try:
            df = pd.read_excel(user_file_path)
        except (pd.errors.EmptyDataError, ValueError): # ValueError por si está vacío o corrupto
             return await update.message.reply_text("ℹ️ Archivo de gastos vacío.")
        if df.empty: return await update.message.reply_text("ℹ️ Archivo de gastos vacío.")

        # Asegurar que la columna Fecha existe y convertirla para ordenar si es necesario
        if 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            # Ordenar por fecha y luego por índice original si las fechas son iguales, para asegurar que el último añadido sea el último
            df = df.sort_values(by='Fecha', ascending=True, na_position='last').reset_index()
        else:
            logger.warning(f"Archivo {user_file_path} no tiene columna 'Fecha'. Eliminando la última fila por índice.")
            # Simplemente usa el índice si no hay fecha

        if df.empty: # Re-chequear por si todas las fechas eran inválidas
            return await update.message.reply_text("ℹ️ No hay operaciones válidas para eliminar.")

        ultima_op_row = df.iloc[-1]
        ultima_op = ultima_op_row.to_dict()
        original_index = ultima_op_row.get('index') # Obtener índice original si se reseteó

        # Releer el archivo original para eliminar por índice original si es posible
        try:
            df_original = pd.read_excel(user_file_path)
            if original_index is not None and original_index in df_original.index:
                df_actualizado = df_original.drop(index=original_index)
                logger.info(f"Eliminando fila con índice original {original_index}")
            else:
                # Si no se pudo usar el índice original, eliminar la última fila leída
                logger.warning(f"No se pudo usar índice original {original_index}, eliminando última fila por posición.")
                df_actualizado = df_original.iloc[:-1]

            # Guardar el DataFrame actualizado
            df_actualizado['Fecha'] = pd.to_datetime(df_actualizado['Fecha'], errors='coerce')
            df_actualizado['Monto'] = pd.to_numeric(df_actualizado['Monto'], errors='coerce')
            df_actualizado.to_excel(user_file_path, index=False, engine='openpyxl')
            logger.info(f"Última operación eliminada por {update.effective_user.id} en {user_file_path}")

            fecha_str = pd.to_datetime(ultima_op.get('Fecha')).strftime('%d/%m/%Y %H:%M') if pd.notna(ultima_op.get('Fecha')) else "N/A"
            respuesta = (f"✅ Última operación eliminada:\n\n"
                         f"🗓 Fecha: {fecha_str}\n"
                         f"👤 Usuario: {ultima_op.get('Usuario', 'N/A')}\n"
                         f"📌 Tipo: {str(ultima_op.get('Tipo', 'N/A')).capitalize()}\n"
                         f"💵 Monto: ${ultima_op.get('Monto', 0):,.2f}\n"
                         f"📝 Descripción: {ultima_op.get('Descripción', 'N/A')}")
            await update.message.reply_text(respuesta)

        except Exception as write_err:
            logger.exception(f"Error al reescribir archivo {user_file_path} después de eliminar última op.")
            await update.message.reply_text("❌ Error al guardar los cambios después de eliminar.")


    except FileNotFoundError: await update.message.reply_text("❌ No se encontró tu archivo de gastos.")
    except Exception as e:
        logger.exception(f"Error al eliminar última operación para {user_file_path}")
        await update.message.reply_text(f"❌ Error inesperado al eliminar: {str(e)}")

@require_authentication
async def descargar_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    if not user_file_path: return await update.message.reply_text("❌ Error interno: archivo no encontrado.")

    user_key = context.user_data.get('user_key_sanitized', 'usuario')
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 Aún no tienes un archivo de gastos generado.")
        # Verificar si el archivo está vacío antes de enviarlo
        try:
            # Comprobar tamaño del archivo primero
            if os.path.getsize(user_file_path) < 50: # Un archivo excel válido suele ser más grande
                 logger.warning(f"Archivo {user_file_path} parece vacío o muy pequeño.")
                 # Intentar leerlo para confirmar
                 try:
                     df = pd.read_excel(user_file_path)
                     if df.empty: return await update.message.reply_text("ℹ️ Tu archivo de gastos está vacío.")
                 except (pd.errors.EmptyDataError, ValueError):
                      return await update.message.reply_text("ℹ️ Tu archivo de gastos está vacío o no se puede leer.")

        except OSError as os_err:
             logger.error(f"Error al acceder al archivo {user_file_path} para descarga: {os_err}")
             return await update.message.reply_text("❌ Error al acceder al archivo para enviarlo.")

        # Si el archivo existe y no está vacío (o la comprobación falló pero existe), intentar enviar
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(user_file_path, 'rb'),
            filename=f"gastos_{user_key}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            caption=f"📊 Tu historial de gastos ({user_key})."
        )
    except FileNotFoundError: await update.message.reply_text("❌ No se encontró tu archivo de gastos (quizás se eliminó?).")
    except Exception as e:
        logger.exception(f"Error al enviar Excel {user_file_path}")
        await update.message.reply_text(f"❌ Error inesperado al enviar el archivo: {str(e)}")

# --- Handlers para Gastos por Texto (Comando /registrargasto y Genérico) ---

@require_authentication
async def registrar_gasto_texto_comando_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Inicia la CONVERSACIÓN para registrar gastos por texto usando /registrargasto."""
    await update.message.reply_text(
        "📝 Ok, envíame los gastos que quieres registrar con el formato:\n"
        "`MONTO en DESCRIPCION (FECHA)`\n\n"
        "Ejemplos:\n"
        "`15000 en perfumes (ayer)`\n"
        "`2100 en verdura.`\n"
        "`18000 en carne (25/12/2023)`\n\n"
        "Puedes poner varios gastos, uno por línea o separados por punto (.).\n"
        "La fecha es opcional (si no la pones, será hoy). Formatos de fecha: hoy, ayer, DD/MM, DD-MM, DD/MM/YY, DD-MM-YY, DD/MM/YYYY, DD-MM-YYYY.\n\n"
        "Usa /cancel si cambias de opinión.",
        parse_mode="Markdown"
    )
    return ESPERANDO_GASTOS_COMANDO # Estado específico para esta conversación

async def procesar_gastos_texto_comando(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handler DENTRO de la conversación de /registrargasto.
    Llama a la lógica reutilizable y termina la conversación.
    """
    logger.info(f"Procesando gastos vía comando /registrargasto para {update.effective_user.id}")
    await _procesar_y_guardar_gasto_texto(update, context)
    return ConversationHandler.END # Termina esta conversación específica

@require_authentication
async def handle_generic_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler para CUALQUIER mensaje de texto que NO sea un comando y
    NO esté siendo manejado por otra conversación activa.
    Intenta procesarlo como un registro de gasto.
    """
    # Importante: Verificar que no estemos en medio de otra conversación
    # Esto es un poco más complejo de verificar directamente aquí.
    # La ESTRUCTURA de cómo añadimos los handlers (Conversations primero)
    # debería prevenir que este handler se active si un ConversationHandler
    # está esperando input.

    # Comprobación adicional: si el texto es muy corto o claramente no un gasto, podríamos ignorarlo.
    text = update.message.text
    if len(text) < 5 or not any(char.isdigit() for char in text):
        logger.debug(f"Mensaje de texto de {update.effective_user.id} ignorado por ser corto o sin números: '{text}'")
        # Podríamos enviar el mensaje de 'unknown' aquí o simplemente no hacer nada
        # await unknown(update, context) # Reutilizar el handler unknown
        # O decidir ignorarlo silenciosamente
        return

    logger.info(f"Procesando mensaje de texto genérico como posible gasto para {update.effective_user.id}")
    await _procesar_y_guardar_gasto_texto(update, context)
    # No devuelve estado porque no es parte de una conversación formal


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancela la operación actual (login, registro de gasto, eliminar gasto)."""
    user = update.message.from_user
    logger.info(f"Usuario {user.first_name} ({user.id}) canceló la conversación.")
    await update.message.reply_text('Operación cancelada.', reply_markup=ReplyKeyboardRemove())
    # Limpiar datos específicos de conversaciones si existen
    context.user_data.pop('prompt_message_id', None)
    context.user_data.pop('gastos_a_eliminar_indices', None)
    return ConversationHandler.END

# --- Handlers de Reportes --- (Sin cambios, solo asegurando @require_authentication)

@require_authentication
async def gasto_semanal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    if not user_file_path: return await update.message.reply_text("❌ Error interno: archivo no encontrado.")
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 Sin registros para generar reporte.")
        try:
             df = pd.read_excel(user_file_path)
        except (pd.errors.EmptyDataError, ValueError):
             return await update.message.reply_text("ℹ️ Archivo de gastos vacío.")
        if df.empty: return await update.message.reply_text("ℹ️ Sin gastos registrados para el reporte semanal.")

        # --- Procesamiento de fechas y montos (más robusto) ---
        if 'Fecha' not in df.columns:
            return await update.message.reply_text("❌ Tu archivo Excel no tiene la columna 'Fecha'. No se puede generar reporte.")
        if 'Monto' not in df.columns:
             return await update.message.reply_text("❌ Tu archivo Excel no tiene la columna 'Monto'. No se puede generar reporte.")
        if 'Tipo' not in df.columns:
             logger.warning(f"Archivo {user_file_path} sin columna 'Tipo'. Se incluirán todos los registros.")
             # Si no hay tipo, asumimos que todo es gasto/compra para el reporte
             df['Tipo'] = 'gasto' # O manejar como prefieras

        try:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            df.dropna(subset=['Fecha'], inplace=True) # Eliminar filas sin fecha válida
            if df.empty: return await update.message.reply_text("⚠️ No se encontraron registros con fechas válidas.")
        except Exception as date_err:
            logger.error(f"Error crítico convirtiendo fechas en {user_file_path}: {date_err}")
            return await update.message.reply_text("⚠️ Ocurrió un error al procesar las fechas de tu archivo.")

        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0)
        # --- Fin procesamiento robusto ---

        hoy = datetime.now()
        inicio_semana = (hoy - timedelta(days=hoy.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        # Filtrar por tipo 'gasto' o 'compra', ignorando mayúsculas/minúsculas
        gastos_semana = df[
            (df['Fecha'] >= inicio_semana) &
            (df['Tipo'].astype(str).str.lower().isin(['gasto', 'compra']))
        ].copy()

        if gastos_semana.empty:
            return await update.message.reply_text(f"ℹ️ No encontré gastos registrados esta semana (desde el {inicio_semana.strftime('%d/%m')}).")

        # Agrupar por descripción (insensible a mayúsculas/minúsculas y espacios)
        gastos_semana['Descripción_Norm'] = gastos_semana['Descripción'].astype(str).str.lower().str.strip()
        total_semana = gastos_semana['Monto'].sum()
        # Usar agg para obtener la suma y la primera descripción original encontrada
        detalles = gastos_semana.groupby('Descripción_Norm').agg(
            Monto_Total=('Monto', 'sum'),
            Descripcion_Original=('Descripción', 'first') # Tomar una de las originales para mostrar
        ).sort_values('Monto_Total', ascending=False)

        respuesta = (f"📊 *Resumen Semanal* ({inicio_semana.strftime('%d/%m/%Y')} - {hoy.strftime('%d/%m/%Y')})\n\n"
                     f"💰 *Gasto Total:* ${total_semana:,.2f}\n\n"
                     f"🔍 *Detalle por concepto:*\n")
        respuesta += "\n".join([f"- {row['Descripcion_Original']}: ${row['Monto_Total']:,.2f}" for _, row in detalles.iterrows()])
        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except FileNotFoundError: await update.message.reply_text("❌ No se encontró tu archivo de gastos.")
    except Exception as e:
        logger.exception(f"Error inesperado en gasto_semanal para {user_file_path}")
        await update.message.reply_text(f"❌ Ocurrió un error inesperado al generar el reporte semanal: {str(e)}")

@require_authentication
async def gasto_mensual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    if not user_file_path: return await update.message.reply_text("❌ Error interno: archivo no encontrado.")

    MESES_ES = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
                7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 Sin registros para generar reporte.")
        try: df = pd.read_excel(user_file_path)
        except (pd.errors.EmptyDataError, ValueError): return await update.message.reply_text("ℹ️ Archivo de gastos vacío.")
        if df.empty: return await update.message.reply_text("ℹ️ Sin gastos registrados para el reporte mensual.")

        # --- Procesamiento de fechas y montos (igual que semanal) ---
        if 'Fecha' not in df.columns: return await update.message.reply_text("❌ Falta columna 'Fecha'.")
        if 'Monto' not in df.columns: return await update.message.reply_text("❌ Falta columna 'Monto'.")
        if 'Tipo' not in df.columns:
            logger.warning(f"Archivo {user_file_path} sin columna 'Tipo'. Incluyendo todo.")
            df['Tipo'] = 'gasto'
        try:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            df.dropna(subset=['Fecha'], inplace=True)
            if df.empty: return await update.message.reply_text("⚠️ Sin fechas válidas.")
        except Exception as date_err:
            logger.error(f"Error convirtiendo fechas {user_file_path}: {date_err}")
            return await update.message.reply_text("⚠️ Error procesando fechas.")
        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0)
        # --- Fin procesamiento robusto ---

        hoy = datetime.now()
        nombre_mes_actual = MESES_ES[hoy.month]
        inicio_mes_actual = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Filtrar por tipo y mes actual
        gastos_mes = df[
            (df['Fecha'] >= inicio_mes_actual) & (df['Fecha'] < (inicio_mes_actual + pd.DateOffset(months=1))) & # Asegurar que sea solo este mes
            (df['Tipo'].astype(str).str.lower().isin(['gasto', 'compra']))
        ].copy()

        if gastos_mes.empty:
             return await update.message.reply_text(f"ℹ️ No encontré gastos registrados en {nombre_mes_actual} de {hoy.year}.")

        gastos_mes['Descripción_Norm'] = gastos_mes['Descripción'].astype(str).str.lower().str.strip()
        total_mes = gastos_mes['Monto'].sum()
        detalles = gastos_mes.groupby('Descripción_Norm').agg(
            Monto_Total=('Monto', 'sum'),
            Descripcion_Original=('Descripción', 'first')
        ).sort_values('Monto_Total', ascending=False)

        respuesta = (f"📅 *Resumen Mensual* ({nombre_mes_actual} {hoy.year})\n\n"
                     f"💰 *Gasto Total:* ${total_mes:,.2f}\n\n"
                     f"🔍 *Detalle por concepto:*\n")
        respuesta += "\n".join([f"- {row['Descripcion_Original']}: ${row['Monto_Total']:,.2f}" for _, row in detalles.iterrows()])
        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except FileNotFoundError: await update.message.reply_text("❌ No se encontró tu archivo de gastos.")
    except Exception as e:
        logger.exception(f"Error inesperado en gasto_mensual para {user_file_path}")
        await update.message.reply_text(f"❌ Ocurrió un error inesperado al generar el reporte mensual: {str(e)}")


# --- Handlers para Eliminar Gasto Específico --- (Sin cambios, solo asegurando @require_authentication)

@require_authentication
async def eliminar_gasto_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Inicia el proceso de eliminación de un gasto específico."""
    user_file_path = get_user_file_path(context)
    if not user_file_path: return await update.message.reply_text("❌ Error interno."), ConversationHandler.END
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 Sin registros."), ConversationHandler.END
        try: df = pd.read_excel(user_file_path)
        except (pd.errors.EmptyDataError, ValueError): return await update.message.reply_text("ℹ️ Archivo vacío."), ConversationHandler.END
        if df.empty: return await update.message.reply_text("ℹ️ Archivo vacío."), ConversationHandler.END

        # --- Procesamiento de fechas (más robusto) ---
        if 'Fecha' not in df.columns: return await update.message.reply_text("❌ Falta columna 'Fecha'."), ConversationHandler.END
        try:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            # Guardar índice original ANTES de eliminar filas con fecha inválida o reordenar
            df['original_index'] = df.index
            df.dropna(subset=['Fecha'], inplace=True)
            if df.empty: return await update.message.reply_text("⚠️ Sin fechas válidas."), ConversationHandler.END
        except Exception as date_err:
             logger.error(f"Error fechas {user_file_path} eliminar: {date_err}")
             return await update.message.reply_text("⚠️ Error procesando fechas."), ConversationHandler.END
        # --- Fin procesamiento ---

        hoy = datetime.now()
        hace_30_dias = hoy - timedelta(days=30)
        # Filtrar por fecha y ordenar por fecha descendente para mostrar los más recientes primero
        gastos_recientes = df[df['Fecha'] >= hace_30_dias].sort_values(by='Fecha', ascending=False).copy()

        if gastos_recientes.empty: return await update.message.reply_text("ℹ️ No encontré gastos registrados en los últimos 30 días."), ConversationHandler.END

        respuesta = "🗑️ Elige el número del gasto que quieres eliminar (mostrando últimos 30 días):\n\n"
        # Guardar el índice ORIGINAL del DataFrame completo para usarlo al eliminar
        gastos_a_eliminar_indices_originales = []
        for i, (_, gasto_row) in enumerate(gastos_recientes.iterrows(), start=1):
             fecha_str = gasto_row['Fecha'].strftime('%d/%m/%y %H:%M') # Más precisión en fecha
             monto = gasto_row.get('Monto', 0)
             desc = gasto_row.get('Descripción', 'N/A')
             respuesta += f"*{i}*) `{fecha_str}` - ${monto:,.2f} - {desc}\n"
             gastos_a_eliminar_indices_originales.append(gasto_row['original_index']) # Guardar índice original

        respuesta += "\nIngresa el número del gasto a borrar o escribe /cancel para salir."
        context.user_data['gastos_a_eliminar_indices'] = gastos_a_eliminar_indices_originales # Guardar índices ORIGINALES
        await update.message.reply_text(respuesta, parse_mode="Markdown")
        return ESPERANDO_NUMERO_ELIMINAR

    except FileNotFoundError: await update.message.reply_text("❌ No se encontró tu archivo."), ConversationHandler.END
    except Exception as e:
        logger.exception(f"Error iniciando eliminacion específica {user_file_path}")
        await update.message.reply_text(f"❌ Error al listar gastos para eliminar: {str(e)}")
        return ConversationHandler.END

async def eliminar_gasto_confirmar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Recibe el número, valida, elimina el gasto usando el índice original y finaliza."""
    user_file_path = get_user_file_path(context)
    if not user_file_path: return await update.message.reply_text("❌ Error interno."), ConversationHandler.END

    texto_usuario = update.message.text
    # Recuperar los índices ORIGINALES guardados
    gastos_a_eliminar_indices_originales = context.user_data.get('gastos_a_eliminar_indices')

    if not gastos_a_eliminar_indices_originales:
        logger.warning(f"Usuario {update.effective_user.id} intentó confirmar eliminación sin lista de índices previa.")
        await update.message.reply_text("🤔 Parece que hubo un problema. Por favor, empieza de nuevo con /eliminargasto.")
        return ConversationHandler.END

    try:
        numero_elegido = int(texto_usuario)
        if not (1 <= numero_elegido <= len(gastos_a_eliminar_indices_originales)):
            raise ValueError("Número fuera de rango")

        # Obtener el ÍNDICE ORIGINAL correspondiente al número elegido por el usuario
        index_to_delete = gastos_a_eliminar_indices_originales[numero_elegido - 1]

        # Releer el archivo COMPLETO para asegurar que eliminamos la fila correcta por su índice original
        try:
             df_completo = pd.read_excel(user_file_path)
             # Verificar que el índice original todavía existe en el DataFrame leído
             if index_to_delete not in df_completo.index:
                  logger.error(f"Índice original {index_to_delete} no encontrado en {user_file_path} al confirmar eliminación. Pudo ser eliminado previamente.")
                  await update.message.reply_text("❌ Error: El gasto seleccionado ya no existe (quizás fue eliminado antes).")
                  context.user_data.pop('gastos_a_eliminar_indices', None) # Limpiar
                  return ConversationHandler.END

             # Guardar datos del gasto ANTES de eliminarlo para mostrar confirmación
             gasto_eliminado = df_completo.loc[index_to_delete].to_dict()

        except (FileNotFoundError, pd.errors.EmptyDataError, ValueError, KeyError) as read_err:
             logger.error(f"Error al releer {user_file_path} o localizar índice {index_to_delete} para eliminar: {read_err}")
             await update.message.reply_text("❌ No se pudo leer el archivo o encontrar el gasto para confirmar la eliminación.")
             context.user_data.pop('gastos_a_eliminar_indices', None) # Limpiar
             return ConversationHandler.END

        # Eliminar la fila usando el índice original
        df_actualizado = df_completo.drop(index=index_to_delete)

        # Guardar el DataFrame actualizado
        df_actualizado['Fecha'] = pd.to_datetime(df_actualizado['Fecha'], errors='coerce')
        df_actualizado['Monto'] = pd.to_numeric(df_actualizado['Monto'], errors='coerce')
        df_actualizado.to_excel(user_file_path, index=False, engine='openpyxl')
        logger.info(f"Usuario {update.effective_user.id} eliminó gasto con índice original {index_to_delete} de {user_file_path}")

        # Mostrar confirmación con los datos del gasto eliminado
        fecha_elim_obj = pd.to_datetime(gasto_eliminado.get('Fecha'), errors='coerce')
        fecha_elim_str = fecha_elim_obj.strftime('%d/%m/%Y %H:%M') if pd.notna(fecha_elim_obj) else "Fecha inválida"
        await update.message.reply_text(
            f"✅ Gasto eliminado con éxito:\n\n"
            f"🗓 Fecha: {fecha_elim_str}\n"
            f"💵 Monto: ${gasto_eliminado.get('Monto', 0):,.2f}\n"
            f"📝 Descripción: {gasto_eliminado.get('Descripción', 'N/A')}"
        )

    except ValueError:
        await update.message.reply_text(f"❌ Número inválido. Debes ingresar un número entre 1 y {len(gastos_a_eliminar_indices_originales)}. Vuelve a intentarlo o usa /cancel.")
        return ESPERANDO_NUMERO_ELIMINAR # Permitir reintento sin reiniciar la conversación
    except Exception as e:
        logger.exception(f"Error inesperado al confirmar eliminación del índice {index_to_delete if 'index_to_delete' in locals() else '??'} en {user_file_path}")
        await update.message.reply_text(f"❌ Ocurrió un error inesperado al intentar eliminar el gasto: {str(e)}")
    finally:
         # Limpiar siempre los índices guardados al salir de esta función (éxito, error o cancelación)
         context.user_data.pop('gastos_a_eliminar_indices', None)

    return ConversationHandler.END # Terminar la conversación de eliminación


# --- Autenticación y Comandos de Sesión --- (Sin cambios significativos)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Inicia la conversación de login o saluda si ya está logueado."""
    user = update.message.from_user
    user_key = context.user_data.get('user_key_sanitized')

    if user_key:
        await update.message.reply_text(f"👋 ¡Hola de nuevo, {user.first_name}! Ya tienes una sesión activa como `{user_key}`.")
        await show_main_menu(update, context)
        return ConversationHandler.END # Ya está autenticado, no necesita entrar en la conversación
    else:
        logger.info(f"Usuario {user.id} ({user.first_name}) iniciando proceso de login.")
        await update.message.reply_text(
            f"¡Bienvenido, {user.first_name}!\n"
            "🔑 Por favor, ingresa tu clave personal para acceder a tus gastos.\n"
            "Si es la primera vez que usas el bot con una clave, se creará un archivo nuevo para ti.\n\n"
            "Tu clave puede contener letras, números y guiones bajos (_).\n"
            "Ejemplos: `gastos_casa`, `mi_cuenta_1`, `juan_perez`\n\n"
            "Si quieres detener el proceso, usa /cancel."
        )
        return ASKING_KEY # Entra en el estado para esperar la clave

async def handle_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Procesa la clave ingresada, la sanitiza y la guarda en user_data."""
    user = update.message.from_user
    potential_key = update.message.text
    sanitized_key = sanitize_key(potential_key)

    if not sanitized_key or sanitized_key == "invalid_key":
        await update.message.reply_text(
            "❌ La clave que ingresaste no es válida.\n"
            "Recuerda usar solo letras, números y guión bajo (_).\n"
            "Inténtalo de nuevo o usa /cancel."
        )
        return ASKING_KEY # Permanece en el estado esperando una clave válida

    user_file_path = os.path.join(USER_DATA_DIR, f"{sanitized_key}.xlsx")

    # Guardar la clave en user_data (será persistido automáticamente)
    context.user_data['user_key_original'] = potential_key
    context.user_data['user_key_sanitized'] = sanitized_key
    context.user_data['user_telegram_id'] = user.id # Guardar también el ID de Telegram

    if os.path.exists(user_file_path):
        logger.info(f"Usuario {user.id} ({user.first_name}) inició sesión con clave existente: {sanitized_key}")
        await update.message.reply_text(f"✅ ¡Perfecto! Has iniciado sesión con la clave '{sanitized_key}'.", reply_markup=ReplyKeyboardRemove())
    else:
        logger.info(f"Usuario {user.id} ({user.first_name}) se registró con nueva clave: {sanitized_key}")
        try:
            # Crear archivo Excel vacío con las columnas esperadas
            df_inicial = pd.DataFrame(columns=['Fecha', 'Usuario', 'Tipo', 'Monto', 'Descripción'])
            df_inicial.to_excel(user_file_path, index=False, engine='openpyxl')
            logger.info(f"Archivo Excel nuevo creado exitosamente: {user_file_path}")
            await update.message.reply_text(f"✨ ¡Listo! Se ha creado un nuevo registro para la clave '{sanitized_key}'.", reply_markup=ReplyKeyboardRemove())
        except Exception as e:
             logger.error(f"Error al crear el archivo Excel inicial para la clave {sanitized_key}: {e}")
             await update.message.reply_text(
                 f"⚠️ Se registró la clave '{sanitized_key}', pero hubo un problema al crear tu archivo de gastos.\n"
                 "Puedes intentar registrar un gasto igualmente, pero si el problema persiste, contacta al administrador.",
                 reply_markup=ReplyKeyboardRemove()
             )

    await show_main_menu(update, context) # Mostrar menú principal después de login/registro exitoso
    return ConversationHandler.END # Termina la conversación de autenticación

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el menú de comandos disponibles al usuario."""
    user_key = context.user_data.get('user_key_sanitized', '???') # Obtener clave actual
    menu_text = (
        f"📌 *Menú Principal* (Sesión: `{user_key}`)\n\n"
        "Puedes registrar gastos de dos formas:\n"
        "1.  🎤 Envía una *nota de voz* o *archivo de audio* diciendo 'gasté X en Y' o 'compré Z por W'.\n"
        "2.  ✍️ Envía un *mensaje de texto* con el formato: `MONTO en DESCRIPCION (FECHA)`\n"
        "    *(La fecha es opcional, ej: `550 en cafe`, `1200.50 en supermercado (ayer)`, `3000 en taxi (15/07/24)`)*\n\n"
        "También puedes usar estos *comandos*:\n"
        "📝 /registrargasto - Inicia un diálogo guiado para añadir gastos por texto.\n"
        "📊 /gastosemanal - Ver resumen de gastos de esta semana.\n"
        "📅 /gastomensual - Ver resumen de gastos de este mes.\n"
        "🗑️ /eliminargasto - Borrar un gasto específico de los últimos 30 días.\n"
        "↩️ /eliminaroperacion - Borrar el *último* gasto o compra registrado.\n"
        "💾 /descargarexcel - Obtener tu archivo Excel con todos los gastos.\n"
        "🚪 /logout - Cerrar tu sesión actual."
    )
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=menu_text,
        parse_mode="Markdown"
    )

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cierra la sesión del usuario limpiando los datos relevantes de user_data."""
    user_key = context.user_data.pop('user_key_sanitized', None)
    context.user_data.pop('user_key_original', None)
    context.user_data.pop('user_telegram_id', None)
    # Limpiar también cualquier estado residual de conversaciones
    context.user_data.pop('gastos_a_eliminar_indices', None)

    if user_key:
        logger.info(f"Usuario {update.effective_user.id} ({update.effective_user.first_name}) cerró sesión de la clave: {user_key}")
        await update.message.reply_text("🔒 Tu sesión ha sido cerrada. Para volver a usar el bot, envía /start.", reply_markup=ReplyKeyboardRemove())
    else:
        logger.info(f"Usuario {update.effective_user.id} ({update.effective_user.first_name}) intentó cerrar sesión sin estar logueado.")
        await update.message.reply_text("🤔 No tenías una sesión activa para cerrar. Puedes iniciar una con /start.", reply_markup=ReplyKeyboardRemove())

# --- Mensaje genérico para comandos/texto no reconocidos ---
async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
     """Manejador para comandos no reconocidos o texto que no encaja en otros handlers."""
     user_key = context.user_data.get('user_key_sanitized')
     message_text = update.message.text

     # Si no está logueado, siempre pedir que use /start
     if not user_key:
         await update.message.reply_text("Hola 👋 Parece que no has iniciado sesión. Usa /start para ingresar con tu clave.")
         return

     # Si está logueado y envió un comando desconocido
     if message_text and message_text.startswith('/'):
         logger.warning(f"Usuario {update.effective_user.id} ({user_key}) envió comando desconocido: {message_text}")
         await update.message.reply_text(f"🤔 No reconozco el comando '{message_text}'.\nRevisa el /start para ver los comandos disponibles o envía texto normal para registrar un gasto.")
     # Si está logueado y envió texto que no fue capturado por handle_generic_text_message
     # (esto podría pasar si handle_generic_text_message decide ignorar textos muy cortos, por ejemplo)
     # O si este handler 'unknown' se activa por alguna otra razón inesperada.
     else:
          logger.info(f"Mensaje de {update.effective_user.id} ({user_key}) no procesado por otros handlers: '{message_text}'")
          # Podrías ofrecer ayuda o simplemente ignorar.
          await update.message.reply_text("🤔 No estoy seguro de qué hacer con eso. Recuerda que puedes:\n"
                                          "- Enviar texto como `100 en algo` para registrar gastos.\n"
                                          "- Enviar una nota de voz.\n"
                                          "- Usar los comandos listados en /start.")


# --- Main Application Setup ---

def main():
    # Intenta obtener el token de una variable de entorno primero
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        # Si no está en variable de entorno, usa el valor hardcoded (NO RECOMENDADO para producción)
        TOKEN = "7686874612:AAEsfK5izb7Y55z_-m5WVB8WBh-JywmN1IU" # Cambia esto si es necesario
        logger.warning("Token del bot cargado directamente desde el código. Es MÁS SEGURO usar variables de entorno.")

    if not TOKEN or TOKEN == "TU_BOT_TOKEN": # Doble chequeo por si acaso
        logger.critical("¡ERROR CRÍTICO! El token del bot de Telegram no está configurado.")
        logger.critical("Configura la variable de entorno 'TELEGRAM_BOT_TOKEN' o edita el código fuente (menos seguro).")
        return

    # --- Configuración de Persistencia ---
    persistence = PicklePersistence(filepath=PERSISTENCE_FILE)
    logger.info(f"Usando PicklePersistence. El estado del bot se guardará en '{PERSISTENCE_FILE}'")

    # --- Construir la Aplicación con Persistencia ---
    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .persistence(persistence) # Aplicar persistencia aquí
        .build()
    )

    # --- Handlers de Conversación ---
    # Estos manejan flujos de varios pasos (login, registrar gasto con comando, eliminar gasto específico)
    auth_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            ASKING_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_key)]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        name="auth_conversation", # Nombre opcional para debugging
        persistent=False # La persistencia del estado de la CONVERSACIÓN no es necesaria aquí, user_data sí es persistente.
    )

    # Conversación para el comando /registrargasto
    gasto_texto_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('registrargasto', registrar_gasto_texto_comando_start)],
        states={
            ESPERANDO_GASTOS_COMANDO: [MessageHandler(filters.TEXT & ~filters.COMMAND, procesar_gastos_texto_comando)]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        name="gasto_texto_comando_conversation",
        persistent=False
    )

    # Conversación para el comando /eliminargasto
    eliminar_gasto_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('eliminargasto', eliminar_gasto_start)],
        states={
            ESPERANDO_NUMERO_ELIMINAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, eliminar_gasto_confirmar)]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        name="eliminar_gasto_conversation",
        persistent=False
    )

    # --- Añadir Handlers a la Aplicación ---
    # El ORDEN es importante. Las conversaciones y comandos específicos deben ir PRIMERO.

    # 1. Handlers de Conversación (manejan /start, /registrargasto, /eliminargasto y sus seguimientos)
    application.add_handler(auth_conv_handler)
    application.add_handler(gasto_texto_conv_handler)
    application.add_handler(eliminar_gasto_conv_handler)

    # 2. Handlers de Comandos Específicos (deben ir después de las conversaciones que los usan como entry point si aplica, pero antes de handlers genéricos)
    application.add_handler(CommandHandler("eliminaroperacion", eliminar_operacion))
    application.add_handler(CommandHandler("descargarexcel", descargar_excel))
    application.add_handler(CommandHandler("gastosemanal", gasto_semanal))
    application.add_handler(CommandHandler("gastomensual", gasto_mensual))
    application.add_handler(CommandHandler("logout", logout))
    # Podríamos añadir un CommandHandler para /start que llame a show_main_menu si ya está logueado,
    # pero el auth_conv_handler ya maneja esto bien.

    # 3. Handlers de Mensajes Específicos (Audio/Voz)
    application.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio))

    # 4. Handler para Texto Genérico (NUEVO - intenta registrar gasto)
    # Este va DESPUÉS de los comandos y conversaciones, para no interceptar /cancel, números para eliminar, etc.
    # Y ANTES del handler 'unknown'.
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_generic_text_message))

    # 5. Handler Final para Comandos/Texto no reconocidos (catch-all)
    # Este debe ser el ÚLTIMO MessageHandler.
    application.add_handler(MessageHandler(filters.COMMAND | filters.TEXT, unknown))

    # --- Iniciar el Bot ---
    logger.info("Bot configurado y listo para iniciar...")
    application.run_polling(allowed_updates=Update.ALL_TYPES) # Escuchar todo tipo de actualizaciones
    logger.info("Bot detenido.")

if __name__ == "__main__":
    main()