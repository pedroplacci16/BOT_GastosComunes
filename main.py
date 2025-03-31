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
# Estados para registro de gastos por texto
ESPERANDO_GASTOS = 3
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
    # context.user_data se carga desde el archivo de persistencia al inicio
    user_key = context.user_data.get('user_key_sanitized')
    if user_key:
        return os.path.join(USER_DATA_DIR, f"{user_key}.xlsx")
    return None

def require_authentication(func):
    """Decorador para asegurar que el usuario esté autenticado."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        # Verifica user_data, que ahora es persistente
        user_file = get_user_file_path(context)
        if not user_file:
            await update.message.reply_text(
                "🔐 Necesitas iniciar sesión primero. Usa /start para ingresar tu clave."
            )
            # Manejo para salir de conversaciones si no está autenticado
            if context.application.conversation_handler is not None:
                 current_state = context.application.conversation_handler.check_update(update)
                 if current_state is not None:
                     return ConversationHandler.END
            return None
        return await func(update, context, *args, **kwargs)
    return wrapper

def normalizar_texto(texto):
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto

# --- Lógica de Procesamiento y Guardado ---

def procesar_texto(texto, usuario):
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
            logger.warning(f"No se pudo convertir monto: {match.group(2)} en texto: {texto}")
    return transacciones

def guardar_en_excel(transacciones, excel_file_path):
    """Guarda las transacciones en el archivo Excel específico del usuario."""
    if not excel_file_path:
        logger.error("Intento de guardar sin ruta de archivo válida.")
        return False
    try:
        df_nuevo = pd.DataFrame(transacciones)
        column_order = ['Fecha', 'Usuario', 'Tipo', 'Monto', 'Descripción']
        df_nuevo = df_nuevo[column_order]

        os.makedirs(os.path.dirname(excel_file_path), exist_ok=True)
        df_final = df_nuevo

        if os.path.exists(excel_file_path):
            try:
                df_existente = pd.read_excel(excel_file_path)
                if not df_existente.empty and all(col in df_existente.columns for col in column_order):
                    for col in column_order:
                        if col not in df_existente.columns: df_existente[col] = pd.NA
                    df_existente = df_existente[column_order]
                    df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
                elif not df_existente.empty:
                     logger.warning(f"Excel {excel_file_path} con formato incorrecto. Se sobrescribirá.")
            except Exception as read_err:
                logger.error(f"Error al leer Excel {excel_file_path}: {read_err}. Se intentará sobrescribir.")

        df_final['Fecha'] = pd.to_datetime(df_final['Fecha'], errors='coerce')
        df_final['Monto'] = pd.to_numeric(df_final['Monto'], errors='coerce')
        df_final.to_excel(excel_file_path, index=False)
        logger.info(f"Transacciones guardadas en {excel_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error CRÍTICO al guardar en Excel {excel_file_path}: {str(e)}")
        return False

# --- Handlers de Comandos (Audio, Descarga, Eliminar Último) ---

@require_authentication
async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    if not transcriptor:
         await update.message.reply_text("❌ Transcripción de audio no configurada.")
         return

    audio_temp_path = "audio_temp_file"
    try:
        user = update.message.from_user
        nombre_usuario = user.first_name or user.username or f"User_{user.id}"
        audio_file = update.message.audio or update.message.voice
        if not audio_file: return

        new_file = await context.bot.get_file(audio_file.file_id)
        file_extension = audio_file.mime_type.split('/')[-1] if audio_file.mime_type else 'oga'
        audio_temp_path_with_ext = f"{audio_temp_path}.{file_extension}"
        await new_file.download_to_drive(audio_temp_path_with_ext)

        texto_transcrito = await transcriptor.transcribir(audio_temp_path_with_ext)
        logger.info(f"Texto transcrito: {texto_transcrito}")
        transacciones = procesar_texto(texto_transcrito, nombre_usuario)

        respuesta = f"🎤 *Usuario*: {nombre_usuario}\n📝 *Transcripción*:\n\n`{texto_transcrito}`\n\n"
        if transacciones:
            if guardar_en_excel(transacciones, user_file_path):
                respuesta += "✅ *Transacciones registradas:*\n" + "\n".join(
                    [f"- {t['Tipo'].capitalize()}: ${t['Monto']:,.2f} - {t['Descripción']}" for t in transacciones])
            else:
                respuesta += "⚠️ Falló el guardado en Excel."
        else:
            respuesta += "ℹ️ No se detectaron transacciones (compre/gaste)."

        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except Exception as e:
        logger.exception(f"Error procesando audio para {user.id if 'user' in locals() else '?'}")
        await update.message.reply_text(f"❌ Error procesando audio: {str(e)}")
    finally:
        possible_paths = [p for p in os.listdir('.') if p.startswith(audio_temp_path)]
        for p in possible_paths:
            try: os.remove(p)
            except OSError as rm_err: logger.error(f"Error eliminando {p}: {rm_err}")

@require_authentication
async def eliminar_operacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Elimina la *última* operación registrada."""
    user_file_path = get_user_file_path(context)
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("❌ No hay operaciones registradas.")
        try: df = pd.read_excel(user_file_path)
        except pd.errors.EmptyDataError: return await update.message.reply_text("ℹ️ Archivo vacío.")
        if df.empty: return await update.message.reply_text("ℹ️ Archivo vacío.")

        ultima_op = df.iloc[-1].to_dict()
        df_actualizado = df.iloc[:-1]

        df_actualizado['Fecha'] = pd.to_datetime(df_actualizado['Fecha'], errors='coerce')
        df_actualizado['Monto'] = pd.to_numeric(df_actualizado['Monto'], errors='coerce')
        df_actualizado.to_excel(user_file_path, index=False)
        logger.info(f"Última operación eliminada por {update.effective_user.id} en {user_file_path}")

        fecha_str = pd.to_datetime(ultima_op.get('Fecha')).strftime('%d/%m/%Y %H:%M') if pd.notna(ultima_op.get('Fecha')) else "N/A"
        respuesta = (f"✅ Última operación eliminada:\n\n"
                     f"🗓 Fecha: {fecha_str}\n"
                     f"👤 Usuario: {ultima_op.get('Usuario', 'N/A')}\n"
                     f"📌 Tipo: {str(ultima_op.get('Tipo', 'N/A')).capitalize()}\n"
                     f"💵 Monto: ${ultima_op.get('Monto', 0):,.2f}\n"
                     f"📝 Descripción: {ultima_op.get('Descripción', 'N/A')}")
        await update.message.reply_text(respuesta)

    except FileNotFoundError: await update.message.reply_text("❌ No se encontró tu archivo.")
    except Exception as e:
        logger.exception(f"Error al eliminar última operación para {user_file_path}")
        await update.message.reply_text(f"❌ Error inesperado: {str(e)}")

@require_authentication
async def descargar_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    user_key = context.user_data.get('user_key_sanitized', 'usuario')
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 No tienes archivo.")
        try:
            df = pd.read_excel(user_file_path)
            if df.empty: return await update.message.reply_text("ℹ️ Archivo vacío.")
        except pd.errors.EmptyDataError: return await update.message.reply_text("ℹ️ Archivo vacío.")
        except Exception as read_err: logger.warning(f"Error leyendo {user_file_path}: {read_err}")

        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(user_file_path, 'rb'),
            filename=f"gastos_{user_key}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            caption=f"📊 Historial de gastos ({user_key})."
        )
    except FileNotFoundError: await update.message.reply_text("❌ No se encontró tu archivo.")
    except Exception as e:
        logger.exception(f"Error al enviar Excel {user_file_path}")
        await update.message.reply_text(f"❌ Error inesperado: {str(e)}")

# --- Handlers para Gastos por Texto ---

@require_authentication
async def registrar_gasto_texto_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📝 Envíame los gastos (monto en descripcion (fecha opcional)):"
        "\nEj: `15000 en perfumes (ayer)`\n    `2100 en verdura.`\n    `18000 en carne (25/12)`"
        "\nUsa '.' o nueva línea para separar. Fecha: hoy, ayer, DD/MM, DD-MM, DD/MM/YY, DD-MM-YY.",
        parse_mode="Markdown"
    )
    return ESPERANDO_GASTOS

async def procesar_gastos_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    if not user_file_path:
        await update.message.reply_text("❌ Error interno: archivo no encontrado. Usa /start.")
        return ConversationHandler.END

    user = update.message.from_user
    nombre_usuario = user.first_name or user.username or f"User_{user.id}"
    texto_completo = update.message.text

    def parsear_fecha(fecha_str: str) -> datetime:
        hoy = datetime.now()
        fecha_str = fecha_str.strip().lower()
        if fecha_str == 'ayer': return (hoy - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
        if fecha_str == 'hoy': return hoy.replace(hour=12, minute=0, second=0, microsecond=0)
        for fmt in ["%d/%m", "%d-%m"]:
            try: return hoy.replace(month=datetime.strptime(fecha_str, fmt).month, day=datetime.strptime(fecha_str, fmt).day, hour=12, minute=0, second=0, microsecond=0)
            except ValueError: continue
        for fmt in ["%d/%m/%y", "%d-%m-%y", "%d/%m/%Y", "%d-%m-%Y"]: # Añadido YYYY
            try:
                dt = datetime.strptime(fecha_str, fmt)
                # Corregir año si es yy < 70 (asumir siglo 21)
                year = dt.year
                if len(fecha_str.split('/')[-1]) == 2 or len(fecha_str.split('-')[-1]) == 2:
                    if dt.year < 1970: year += 100 # Ajuste simple, podría ser más sofisticado
                return dt.replace(year=year, hour=12, minute=0, second=0, microsecond=0)
            except ValueError: continue
        raise ValueError(f"Formato fecha no soportado: '{fecha_str}'")

    transacciones_procesadas = []
    errores = []
    lineas = re.split(r'[.\n]+', texto_completo)
    patron = r'^\s*([\d.,]+)\s+en\s+(.+?)(?:\s+\((.+)\))?\s*$'

    for i, linea in enumerate(lineas, 1):
        linea_limpia = linea.strip()
        if not linea_limpia: continue
        match = re.match(patron, linea_limpia, re.IGNORECASE)
        if not match:
            errores.append(f"Línea {i}: Formato incorrecto -> '{linea_limpia}'")
            continue
        monto_str, descripcion, fecha_str_raw = match.groups()
        fecha_str = fecha_str_raw or 'hoy'
        try:
            monto_str_limpio = monto_str.replace('.', '').replace(',', '.') if ',' in monto_str else monto_str.replace('.', '')
            monto = float(monto_str_limpio)
            if monto <= 0: raise ValueError("Monto debe ser positivo")
        except ValueError as e:
            errores.append(f"Línea {i}: Monto inválido '{monto_str}' -> {e}")
            continue
        try:
            fecha = parsear_fecha(fecha_str.strip())
            if fecha > datetime.now() + timedelta(days=2): raise ValueError("Fecha futura")
        except ValueError as e:
            errores.append(f"Línea {i}: Fecha inválida '{fecha_str}' -> {e}")
            continue
        descripcion_limpia = descripcion.strip().capitalize()
        transacciones_procesadas.append({
            "Tipo": "gasto", "Monto": monto, "Descripción": descripcion_limpia,
            "Usuario": nombre_usuario, "Fecha": fecha.strftime("%Y-%m-%d %H:%M:%S")})

    respuesta = ""
    if transacciones_procesadas:
        if guardar_en_excel(transacciones_procesadas, user_file_path):
            respuesta += "✅ Gastos registrados:\n" + "\n".join(
                [f"- ${t['Monto']:,.2f} en {t['Descripción']} ({datetime.strptime(t['Fecha'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')})"
                 for t in transacciones_procesadas])
        else: respuesta += "⚠️ Falló guardado en Excel.\n"
    else: respuesta += "ℹ️ No se encontraron gastos válidos.\n"
    if errores: respuesta += "\n\n❌ Errores:\n" + "\n".join(errores) + "\n\nRevisa formato: `MONTO en DESCRIPCION (FECHA)`"
    await update.message.reply_text(respuesta, parse_mode="Markdown")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancela la operación actual (login, registro de gasto, eliminar gasto)."""
    user = update.message.from_user
    logger.info(f"Usuario {user.first_name} ({user.id}) canceló la conversación.")
    await update.message.reply_text('Operación cancelada.', reply_markup=ReplyKeyboardRemove())
    context.user_data.pop('prompt_message_id', None)
    context.user_data.pop('gastos_a_eliminar_indices', None)
    return ConversationHandler.END

# --- Handlers de Reportes ---

@require_authentication
async def gasto_semanal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 Sin registros.")
        try: df = pd.read_excel(user_file_path)
        except pd.errors.EmptyDataError: return await update.message.reply_text("ℹ️ Archivo vacío.")
        if df.empty: return await update.message.reply_text("ℹ️ Sin gastos para reporte.")

        try:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            df.dropna(subset=['Fecha'], inplace=True)
            if df.empty: return await update.message.reply_text("⚠️ Sin fechas válidas.")
        except KeyError: return await update.message.reply_text("❌ Falta columna 'Fecha'.")
        except Exception as date_err:
            logger.error(f"Error convirtiendo fechas {user_file_path}: {date_err}")
            return await update.message.reply_text("⚠️ Error procesando fechas.")

        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0)
        hoy = datetime.now()
        inicio_semana = (hoy - timedelta(days=hoy.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        gastos_semana = df[(df['Fecha'] >= inicio_semana) & (df['Tipo'].str.lower().isin(['gasto', 'compra']))].copy()

        if gastos_semana.empty: return await update.message.reply_text(f"ℹ️ Sin gastos esta semana (desde {inicio_semana.strftime('%d/%m')}).")

        gastos_semana['Descripción_Lower'] = gastos_semana['Descripción'].str.lower().str.strip()
        total = gastos_semana['Monto'].sum()
        detalles = gastos_semana.groupby('Descripción_Lower').agg(
            Monto_Total=('Monto', 'sum'), Descripcion_Original=('Descripción', 'first')
        ).sort_values('Monto_Total', ascending=False)

        respuesta = (f"📊 *Resumen Semanal* ({inicio_semana.strftime('%d/%m')} - {hoy.strftime('%d/%m')})\n\n"
                     f"💰 *Total:* ${total:,.2f}\n\n🔍 Detalles:\n")
        respuesta += "\n".join([f"- {row['Descripcion_Original']}: ${row['Monto_Total']:,.2f}" for _, row in detalles.iterrows()])
        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except FileNotFoundError: await update.message.reply_text("❌ No se encontró archivo.")
    except Exception as e:
        logger.exception(f"Error en gasto_semanal para {user_file_path}")
        await update.message.reply_text(f"❌ Error inesperado: {str(e)}")

@require_authentication
async def gasto_mensual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_file_path = get_user_file_path(context)
    MESES_ES = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 Sin registros.")
        try: df = pd.read_excel(user_file_path)
        except pd.errors.EmptyDataError: return await update.message.reply_text("ℹ️ Archivo vacío.")
        if df.empty: return await update.message.reply_text("ℹ️ Sin gastos para reporte.")

        try:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            df.dropna(subset=['Fecha'], inplace=True)
            if df.empty: return await update.message.reply_text("⚠️ Sin fechas válidas.")
        except KeyError: return await update.message.reply_text("❌ Falta columna 'Fecha'.")
        except Exception as date_err:
            logger.error(f"Error convirtiendo fechas {user_file_path}: {date_err}")
            return await update.message.reply_text("⚠️ Error procesando fechas.")

        df['Monto'] = pd.to_numeric(df['Monto'], errors='coerce').fillna(0)
        hoy = datetime.now()
        nombre_mes = MESES_ES[hoy.month]
        inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        gastos_mes = df[(df['Fecha'] >= inicio_mes) & (df['Tipo'].str.lower().isin(['gasto', 'compra']))].copy()

        if gastos_mes.empty: return await update.message.reply_text(f"ℹ️ Sin gastos en {nombre_mes} {hoy.year}.")

        gastos_mes['Descripción_Lower'] = gastos_mes['Descripción'].str.lower().str.strip()
        total = gastos_mes['Monto'].sum()
        detalles = gastos_mes.groupby('Descripción_Lower').agg(
            Monto_Total=('Monto', 'sum'), Descripcion_Original=('Descripción', 'first')
        ).sort_values('Monto_Total', ascending=False)

        respuesta = (f"📅 *Resumen Mensual* ({nombre_mes} {hoy.year})\n\n"
                     f"💰 *Total:* ${total:,.2f}\n\n🔍 Detalles:\n")
        respuesta += "\n".join([f"- {row['Descripcion_Original']}: ${row['Monto_Total']:,.2f}" for _, row in detalles.iterrows()])
        await update.message.reply_text(respuesta, parse_mode="Markdown")

    except FileNotFoundError: await update.message.reply_text("❌ No se encontró archivo.")
    except Exception as e:
        logger.exception(f"Error en gasto_mensual para {user_file_path}")
        await update.message.reply_text(f"❌ Error inesperado: {str(e)}")

# --- Handlers para Eliminar Gasto Específico ---

@require_authentication
async def eliminar_gasto_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Inicia el proceso de eliminación de un gasto específico."""
    user_file_path = get_user_file_path(context)
    try:
        if not os.path.exists(user_file_path): return await update.message.reply_text("📭 Sin registros."), ConversationHandler.END
        try: df = pd.read_excel(user_file_path)
        except pd.errors.EmptyDataError: return await update.message.reply_text("ℹ️ Archivo vacío."), ConversationHandler.END
        if df.empty: return await update.message.reply_text("ℹ️ Archivo vacío."), ConversationHandler.END

        try:
            df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
            df.dropna(subset=['Fecha'], inplace=True)
            if df.empty: return await update.message.reply_text("⚠️ Sin fechas válidas."), ConversationHandler.END
        except KeyError: return await update.message.reply_text("❌ Falta columna 'Fecha'."), ConversationHandler.END
        except Exception as date_err:
             logger.error(f"Error fechas {user_file_path} eliminar: {date_err}")
             return await update.message.reply_text("⚠️ Error procesando fechas."), ConversationHandler.END

        hoy = datetime.now()
        hace_30_dias = hoy - timedelta(days=30)
        gastos_recientes = df[df['Fecha'] >= hace_30_dias].sort_values(by='Fecha', ascending=False).copy()

        if gastos_recientes.empty: return await update.message.reply_text("ℹ️ Sin gastos últimos 30 días."), ConversationHandler.END

        respuesta = "🗑️ Elige el nº de gasto a eliminar (últimos 30 días):\n\n"
        gastos_a_eliminar_indices = []
        for i, (original_index, gasto) in enumerate(gastos_recientes.iterrows(), start=1):
             fecha_str = gasto['Fecha'].strftime('%d/%m')
             monto = gasto.get('Monto', 0)
             desc = gasto.get('Descripción', 'N/A')
             respuesta += f"{i}) `{fecha_str}` - ${monto:,.2f} - {desc}\n"
             gastos_a_eliminar_indices.append(original_index)

        respuesta += "\nIngresa número o /cancel."
        context.user_data['gastos_a_eliminar_indices'] = gastos_a_eliminar_indices # Guardar para el siguiente paso
        await update.message.reply_text(respuesta, parse_mode="Markdown")
        return ESPERANDO_NUMERO_ELIMINAR

    except FileNotFoundError: await update.message.reply_text("❌ No se encontró archivo."), ConversationHandler.END
    except Exception as e:
        logger.exception(f"Error iniciando eliminacion {user_file_path}")
        await update.message.reply_text(f"❌ Error listando gastos: {str(e)}")
        return ConversationHandler.END

async def eliminar_gasto_confirmar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Recibe el número, valida, elimina el gasto y finaliza."""
    user_file_path = get_user_file_path(context)
    texto_usuario = update.message.text
    gastos_a_eliminar_indices = context.user_data.get('gastos_a_eliminar_indices')

    if not gastos_a_eliminar_indices:
        logger.warning(f"Usuario {update.effective_user.id} confirmó sin lista previa.")
        await update.message.reply_text("🤔 Problema interno. Empieza de nuevo con /eliminargasto.")
        return ConversationHandler.END

    try:
        numero_elegido = int(texto_usuario)
        if not (1 <= numero_elegido <= len(gastos_a_eliminar_indices)): raise ValueError("Número fuera rango")
        index_to_delete = gastos_a_eliminar_indices[numero_elegido - 1]

        # Releer archivo por seguridad
        try:
             df_completo = pd.read_excel(user_file_path)
             if index_to_delete not in df_completo.index:
                  logger.error(f"Índice {index_to_delete} no encontrado {user_file_path} al confirmar.")
                  await update.message.reply_text("❌ Error: Gasto no encontrado (quizás ya se eliminó).")
                  context.user_data.pop('gastos_a_eliminar_indices', None)
                  return ConversationHandler.END
             gasto_eliminado = df_completo.loc[index_to_delete].to_dict()
        except (FileNotFoundError, pd.errors.EmptyDataError):
             await update.message.reply_text("❌ No se pudo leer archivo para eliminar.")
             context.user_data.pop('gastos_a_eliminar_indices', None)
             return ConversationHandler.END

        df_actualizado = df_completo.drop(index=index_to_delete)
        df_actualizado['Fecha'] = pd.to_datetime(df_actualizado['Fecha'], errors='coerce')
        df_actualizado['Monto'] = pd.to_numeric(df_actualizado['Monto'], errors='coerce')
        df_actualizado.to_excel(user_file_path, index=False)
        logger.info(f"Usuario {update.effective_user.id} eliminó índice {index_to_delete} de {user_file_path}")

        fecha_elim = pd.to_datetime(gasto_eliminado.get('Fecha')).strftime('%d/%m/%Y') if pd.notna(gasto_eliminado.get('Fecha')) else "N/A"
        await update.message.reply_text(
            f"✅ Gasto eliminado:\n"
            f"- Fecha: {fecha_elim}\n"
            f"- Monto: ${gasto_eliminado.get('Monto', 0):,.2f}\n"
            f"- Desc: {gasto_eliminado.get('Descripción', 'N/A')}"
        )
    except ValueError:
        await update.message.reply_text(f"❌ Número inválido. Ingresa nº entre 1 y {len(gastos_a_eliminar_indices)}, o /cancel.")
        return ESPERANDO_NUMERO_ELIMINAR # Reintentar
    except Exception as e:
        logger.exception(f"Error confirmando eliminación {user_file_path}")
        await update.message.reply_text(f"❌ Error inesperado al eliminar: {str(e)}")
    finally:
         context.user_data.pop('gastos_a_eliminar_indices', None) # Limpiar siempre
    return ConversationHandler.END

# --- Autenticación y Comandos de Sesión ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Inicia la conversación de login o saluda si ya está logueado."""
    user = update.message.from_user
    # user_data se carga desde el archivo de persistencia si existe
    user_key = context.user_data.get('user_key_sanitized')

    if user_key:
        await update.message.reply_text(f"👋 ¡Hola {user.first_name}! Sesión activa.")
        await show_main_menu(update, context)
        # Si ya está logueado, no entramos en la conversación de login
        return ConversationHandler.END
    else:
        logger.info(f"Usuario {user.id} ({user.first_name}) iniciando login.")
        await update.message.reply_text(
            f"¡Bienvenido {user.first_name}!\n🔑 Ingresa tu clave personal (o crea una nueva).\n"
            "Ej: 'casa_perez', 'mis_gastos_1'. Usa /cancel para salir."
        )
        # Entramos en la conversación para pedir la clave
        return ASKING_KEY

async def handle_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Procesa la clave y guarda en user_data (que será persistido)."""
    user = update.message.from_user
    potential_key = update.message.text
    sanitized_key = sanitize_key(potential_key)

    if not sanitized_key or sanitized_key == "invalid_key":
        await update.message.reply_text("❌ Clave inválida (letras, números, _). Intenta de nuevo o /cancel.")
        return ASKING_KEY # Sigue esperando

    user_file_path = os.path.join(USER_DATA_DIR, f"{sanitized_key}.xlsx")
    # Guardar en user_data. PicklePersistence se encargará de escribirlo al archivo.
    context.user_data['user_key_original'] = potential_key
    context.user_data['user_key_sanitized'] = sanitized_key
    context.user_data['user_telegram_id'] = user.id

    if os.path.exists(user_file_path):
        logger.info(f"Usuario {user.id} logueado con clave: {sanitized_key}")
        await update.message.reply_text(f"✅ Sesión iniciada como '{sanitized_key}'.", reply_markup=ReplyKeyboardRemove())
    else:
        logger.info(f"Usuario {user.id} registrado con nueva clave: {sanitized_key}")
        try:
            pd.DataFrame(columns=['Fecha', 'Usuario', 'Tipo', 'Monto', 'Descripción']).to_excel(user_file_path, index=False)
            logger.info(f"Archivo Excel creado: {user_file_path}")
            await update.message.reply_text(f"✨ Nueva clave '{sanitized_key}' creada.", reply_markup=ReplyKeyboardRemove())
        except Exception as e:
             logger.error(f"No se pudo crear Excel inicial para {sanitized_key}: {e}")
             await update.message.reply_text(f"⚠️ Usuario '{sanitized_key}' creado, pero hubo error al crear archivo. Intenta registrar gasto.", reply_markup=ReplyKeyboardRemove())

    await show_main_menu(update, context)
    return ConversationHandler.END # Termina conversación de login

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra menú de comandos disponibles."""
    user_key = context.user_data.get('user_key_sanitized', '')
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📌 *Comandos:*\n"
             "🎤 Envía *audio* para registrar.\n"
             "📝 /registrargasto - Añadir por texto.\n"
             "📊 /gastosemanal - Resumen semana.\n"
             "📅 /gastomensual - Resumen mes.\n"
             "🗑️ /eliminargasto - Borrar gasto (30 días).\n"
             "↩️ /eliminaroperacion - Borrar *último* registro.\n"
             "💾 /descargarexcel - Bajar Excel.\n"
             f"👤 Sesión: `{user_key}`\n"
             "🚪 /logout - Cerrar sesión.",
        parse_mode="Markdown"
    )

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cierra sesión limpiando user_data (cambio será persistido)."""
    user_key = context.user_data.pop('user_key_sanitized', None)
    context.user_data.pop('user_key_original', None)
    context.user_data.pop('user_telegram_id', None)
    context.user_data.pop('gastos_a_eliminar_indices', None) # Limpiar estado de conversaciones

    if user_key:
        logger.info(f"Usuario {update.effective_user.id} cerró sesión ({user_key})")
        await update.message.reply_text("🔒 Sesión cerrada. Usa /start para volver.", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("🤔 No tenías sesión activa.", reply_markup=ReplyKeyboardRemove())

# --- Main Application Setup ---

def main():
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "7686874612:AAEsfK5izb7Y55z_-m5WVB8WBh-JywmN1IU") # ¡Usa variable de entorno!
    if TOKEN == "TU_BOT_TOKEN":
        logger.critical("¡ERROR! Reemplaza 'TU_BOT_TOKEN' con el token real.")
        return

    # --- Configuración de Persistencia ---
    # Guardará user_data, chat_data, bot_data en el archivo especificado.
    # ¡Importante! No compartas este archivo si contiene información sensible.
    persistence = PicklePersistence(filepath=PERSISTENCE_FILE)
    logger.info(f"Usando PicklePersistence. Estado guardado en '{PERSISTENCE_FILE}'")

    # --- Construir la Aplicación con Persistencia ---
    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .persistence(persistence) # ¡Aquí se aplica la persistencia!
        .build()
    )

    # --- Handlers de Conversación ---
    # El estado del login (en user_data) es manejado por la persistencia general.
    # No es estrictamente necesario hacer los ConversationHandlers persistentes
    # para este caso de uso, pero podría ser útil si las conversaciones fueran largas.
    auth_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={ASKING_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_key)]},
        fallbacks=[CommandHandler('cancel', cancel)],
        # Opcional: conversation_timeout=300
        # Opcional: name="auth_conv", persistent=True
    )
    gasto_texto_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('registrargasto', registrar_gasto_texto_start)],
        states={ESPERANDO_GASTOS: [MessageHandler(filters.TEXT & ~filters.COMMAND, procesar_gastos_texto)]},
        fallbacks=[CommandHandler('cancel', cancel)],
        # Opcional: name="gasto_texto_conv", persistent=True
    )
    eliminar_gasto_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('eliminargasto', eliminar_gasto_start)],
        states={ESPERANDO_NUMERO_ELIMINAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, eliminar_gasto_confirmar)]},
        fallbacks=[CommandHandler('cancel', cancel)],
        # Opcional: conversation_timeout=300
        # Opcional: name="eliminar_gasto_conv", persistent=True
    )

    # --- Añadir Handlers a la Aplicación ---
    application.add_handler(auth_conv_handler) # Maneja /start para login o saludo
    application.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio))
    application.add_handler(CommandHandler("eliminaroperacion", eliminar_operacion))
    application.add_handler(CommandHandler("descargarexcel", descargar_excel))
    application.add_handler(CommandHandler("gastosemanal", gasto_semanal))
    application.add_handler(CommandHandler("gastomensual", gasto_mensual))
    application.add_handler(CommandHandler("logout", logout))
    application.add_handler(gasto_texto_conv_handler) # Conversación para registrar texto
    application.add_handler(eliminar_gasto_conv_handler) # Conversación para eliminar específico

    # --- Mensaje genérico para comandos no reconocidos ---
    async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
         # La verificación de user_data funciona con o sin persistencia
         if not context.user_data.get('user_key_sanitized'):
             await update.message.reply_text("Hola 👋 Usa /start para ingresar con tu clave.")
         else:
             await update.message.reply_text("🤔 Comando no reconocido. Usa /start para ver opciones o /cancel.")

    application.add_handler(MessageHandler(filters.COMMAND | filters.TEXT, unknown))

    logger.info("Bot iniciado con persistencia. Esperando comandos...")
    # Iniciar el bot
    application.run_polling()

if __name__ == "__main__":
    main()