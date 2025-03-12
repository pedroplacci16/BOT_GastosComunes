import os
import whisper
import pandas as pd
import re
import unicodedata
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

modelo = "base"
EXCEL_FILE = "gastos.xlsx"

def normalizar_texto(texto):
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    return texto

def procesar_texto(texto, usuario):
    texto_normalizado = normalizar_texto(texto)
    transacciones = []
    
    # Patrón mejorado para capturar monto y descripción
    patron = r'\b(compre|gaste)\b[^\d]*([\d\.,]+)(.*?)(?=\b(?:compre|gaste)\b|$)'
    matches = re.finditer(patron, texto_normalizado, re.DOTALL | re.IGNORECASE)
    
    for match in matches:
        tipo = "compra" if match.group(1).lower() == "compre" else "gasto"
        cantidad_str = match.group(2).strip()
        descripcion = match.group(3).strip()
        
        # Limpiar y formatear la descripción
        descripcion = re.sub(r'^\W+', '', descripcion)  # Quitar caracteres especiales al inicio
        descripcion = re.sub(r'\s+', ' ', descripcion)  # Eliminar múltiples espacios
        descripcion = descripcion.capitalize()
        
        if not descripcion:
            descripcion = "Sin descripción"
            
        # Procesar el monto
        cantidad_str = cantidad_str.replace('.', '').replace(',', '.')
        
        try:
            cantidad = float(cantidad_str)
            transacciones.append({
                "Tipo": tipo,
                "Monto": cantidad,
                "Descripción": descripcion,
                "Usuario": usuario,
                "Fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        except ValueError:
            logging.warning(f"No se pudo convertir el monto: {cantidad_str}")
    
    return transacciones

def guardar_en_excel(transacciones):
    try:
        df_nuevo = pd.DataFrame(transacciones)
        
        # Asegurar el orden de las columnas
        column_order = ['Fecha', 'Usuario', 'Tipo', 'Monto', 'Descripción']
        df_nuevo = df_nuevo[column_order]
        
        if os.path.exists(EXCEL_FILE):
            df_existente = pd.read_excel(EXCEL_FILE)
            
            # Si el Excel existente no tiene las columnas nuevas, agregarlas
            for col in column_order:
                if col not in df_existente.columns:
                    df_existente[col] = None
            
            df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
        else:
            df_final = df_nuevo
        
        df_final.to_excel(EXCEL_FILE, index=False)
        return True
    except Exception as e:
        logging.error(f"Error al guardar en Excel: {str(e)}")
        return False

async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.message.from_user
        nombre_usuario = user.first_name or user.username or "Usuario desconocido"
        
        audio_file = update.message.audio or update.message.voice
        new_file = await context.bot.get_file(audio_file.file_id)
        await new_file.download_to_drive("audio")
        
        model = whisper.load_model(modelo)
        result = model.transcribe("audio")
        texto_transcrito = result['text']
        
        transacciones = procesar_texto(texto_transcrito, nombre_usuario)
        respuesta = f"🎤 *Usuario*: {nombre_usuario}\n📝 *Transcripción*:\n\n{texto_transcrito}\n\n"
        
        if transacciones:
            if guardar_en_excel(transacciones):
                respuesta += "✅ *Transacciones registradas:*\n"
                for t in transacciones:
                    respuesta += f"- {t['Tipo'].capitalize()}: ${t['Monto']:.2f} - {t['Descripción']}\n"
            else:
                respuesta += "⚠️ Se detectaron transacciones pero falló el guardado en Excel"
        else:
            respuesta += "ℹ️ No se detectaron transacciones (compre/gaste)"
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=respuesta,
            parse_mode="Markdown"
        )
        
    except Exception as e:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"❌ Error al procesar el audio: {str(e)}"
        )
    finally:
        if os.path.exists("audio"):
            os.remove("audio")
from telegram.ext import CommandHandler

async def eliminar_operacion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not os.path.exists(EXCEL_FILE):
            await update.message.reply_text("❌ No hay operaciones registradas")
            return

        df = pd.read_excel(EXCEL_FILE)
        
        if df.empty:
            await update.message.reply_text("ℹ️ El archivo de gastos está vacío")
            return

        # Obtener última operación
        ultima_op = df.iloc[-1].to_dict()
        
        # Eliminar última fila
        df = df.iloc[:-1]
        
        # Guardar cambios
        df.to_excel(EXCEL_FILE, index=False)
        
        # Crear mensaje de respuesta
        respuesta = (
            "✅ Última operación eliminada:\n\n"
            f"🗓 Fecha: {ultima_op['Fecha']}\n"
            f"👤 Usuario: {ultima_op['Usuario']}\n"
            f"📌 Tipo: {ultima_op['Tipo'].capitalize()}\n"
            f"💵 Monto: ${ultima_op['Monto']:.2f}\n"
            f"📝 Descripción: {ultima_op.get('Descripción', 'Sin descripción')}"
        )
        
        await update.message.reply_text(respuesta)

    except Exception as e:
        logging.error(f"Error al eliminar operación: {str(e)}")
        await update.message.reply_text("❌ Error al eliminar la última operación")
async def descargar_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not os.path.exists(EXCEL_FILE):
            await update.message.reply_text("📭 No hay archivo de gastos registrado")
            return

        # Verificar si el archivo está vacío
        df = pd.read_excel(EXCEL_FILE)
        if df.empty:
            await update.message.reply_text("ℹ️ El archivo de gastos está vacío")
            return

        # Enviar el archivo Excel
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(EXCEL_FILE, 'rb'),
            filename="gastos.xlsx",
            caption="📊 Aquí tienes tu historial de gastos/compas"
        )

    except Exception as e:
        logging.error(f"Error al enviar Excel: {str(e)}")
        await update.message.reply_text("❌ Error al descargar el archivo Excel")

from telegram.ext import ConversationHandler, CommandHandler, MessageHandler
import re

# Estados de la conversación
ESPERANDO_GASTOS = 1

async def registrar_gasto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📝 Envíame los gastos en este formato:\n\n"
        "Ejemplo:\n"
        "15000 en perfumes. 2100 en verdura, tomate. 18000 en carne, bondiola\n\n"
        "Usa puntos para separar cada gasto y 'en' para separar el monto de la descripción. USAR PARENTESIS SOLO PARA ESPECIFICAR LA FECHA DEL GASTO"
    )
    return ESPERANDO_GASTOS

async def procesar_gastos_texto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    nombre_usuario = user.first_name or user.username or "Usuario desconocido"
    texto = update.message.text
    
    def parsear_fecha(fecha_str: str) -> datetime:
        hoy = datetime.now()
        fecha_str = fecha_str.strip().lower()
        
        # Manejar fechas relativas
        if fecha_str == 'ayer':
            return hoy - timedelta(days=1)
        if fecha_str == 'hoy':
            return hoy
        
        # Manejar diferentes formatos de fecha
        try:
            # Formato DD-MM
            if '-' in fecha_str and len(fecha_str.split('-')) == 2:
                day, month = map(int, fecha_str.split('-'))
                return hoy.replace(month=month, day=day, hour=hoy.hour, minute=hoy.minute)
            
            # Formato DD
            if len(fecha_str) <= 2:
                day = int(fecha_str)
                return hoy.replace(day=day)
            
            # Formato DD/MM
            if '/' in fecha_str and len(fecha_str.split('/')) == 2:
                day, month = map(int, fecha_str.split('/'))
                return hoy.replace(month=month, day=day, hour=hoy.hour, minute=hoy.minute)
            
        except ValueError:
            raise ValueError(f"Formato de fecha inválido: {fecha_str}")
        
        # Si no coincide con ningún formato
        raise ValueError(f"Formato de fecha no reconocido: {fecha_str}")

    try:
        transacciones_raw = [t.strip() for t in texto.split('.') if t.strip()]
        transacciones_procesadas = []
        
        # Nuevo patrón que incluye fecha opcional
        patron = r'^\s*([\d\.,]+)\s+en\s+(.+?)(?:\s+\((.+?)\))?\s*$'
        
        for i, trans in enumerate(transacciones_raw, 1):
            match = re.match(patron, trans, re.IGNORECASE)
            if not match:
                raise ValueError(f"Formato incorrecto en transacción {i}: '{trans}'")
            
            monto_str, descripcion, fecha_str = match.groups()
            fecha_str = fecha_str or 'hoy'  # Default a fecha actual
            
            # Procesar monto
            monto_str = monto_str.replace(',', '.')
            if '.' in monto_str and ',' not in monto_str:
                monto_str = monto_str.replace('.', '')
            monto = float(monto_str)
            
            # Procesar fecha
            try:
                fecha = parsear_fecha(fecha_str)
            except Exception as e:
                raise ValueError(f"Error en fecha de transacción {i}: {str(e)}")
            
            # Validar fecha lógica
            if fecha > datetime.now() + timedelta(days=1):
                raise ValueError(f"Fecha futura no permitida en transacción {i}")
            
            transacciones_procesadas.append({
                "Tipo": "gasto",
                "Monto": monto,
                "Descripción": descripcion.capitalize(),
                "Usuario": nombre_usuario,
                "Fecha": fecha.strftime("%Y-%m-%d %H:%M:%S")
            })
        
        if transacciones_procesadas:
            guardar_en_excel(transacciones_procesadas)
            respuesta = "✅ Gastos registrados:\n" + "\n".join(
                [f"- ${t['Monto']:.2f} en {t['Descripción']} ({datetime.strptime(t['Fecha'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')})"
                 for t in transacciones_procesadas]
            )
        else:
            respuesta = "ℹ️ No se encontraron gastos válidos para registrar"
        
    except Exception as e:
        respuesta = f"❌ Error: {str(e)}"
    
    await update.message.reply_text(respuesta)
    return ConversationHandler.END

async def gasto_semanal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not os.path.exists(EXCEL_FILE):
            await update.message.reply_text("📭 No hay registros de gastos aún")
            return

        df = pd.read_excel(EXCEL_FILE)
        
        # Convertir la columna de fecha a datetime
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        
        # Obtener fecha actual
        hoy = datetime.now()
        
        # Calcular inicio de la semana (lunes)
        inicio_semana = hoy - timedelta(days=hoy.weekday())
        inicio_semana = inicio_semana.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Filtrar gastos de la semana actual
        gastos_semana = df[
            (df['Fecha'] >= inicio_semana) & 
            (df['Tipo'].str.lower().isin(['gasto', 'compra']))
        ]
        
        if gastos_semana.empty:
            await update.message.reply_text("ℹ️ No hay gastos registrados esta semana")
            return
        
        # Calcular total y resumen
        total = gastos_semana['Monto'].sum()
        detalles = gastos_semana.groupby('Descripción')['Monto'].sum().to_dict()
        
        # Formatear respuesta
        respuesta = (
            f"📊 *Resumen Semanal* ({inicio_semana.strftime('%d/%m')} - {hoy.strftime('%d/%m')})\n\n"
            f"💰 *Total gastado:* ${total:.2f}\n\n"
            "🔍 Detalles por categoría:\n"
        )
        
        for categoria, monto in detalles.items():
            respuesta += f"- {categoria}: ${monto:.2f}\n"
        
        await update.message.reply_text(respuesta, parse_mode="Markdown")
        
    except Exception as e:
        logging.error(f"Error en gasto_semanal: {str(e)}")
        await update.message.reply_text("❌ Error al calcular el gasto semanal")

async def gasto_mensual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Diccionario de meses en español
        MESES_ES = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }

        if not os.path.exists(EXCEL_FILE):
            await update.message.reply_text("📭 No hay registros de gastos aún")
            return

        df = pd.read_excel(EXCEL_FILE)
        
        # Convertir la columna de fecha a datetime
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        
        # Obtener fecha actual
        hoy = datetime.now()
        nombre_mes = MESES_ES[hoy.month]  # Obtener nombre del mes del diccionario
        
        # Calcular inicio del mes
        inicio_mes = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Filtrar gastos del mes actual
        gastos_mes = df[
            (df['Fecha'] >= inicio_mes) & 
            (df['Tipo'].str.lower().isin(['gasto', 'compra']))
        ]
        
        if gastos_mes.empty:
            await update.message.reply_text(f"ℹ️ No hay gastos registrados en {nombre_mes}")
            return
        
        # Calcular total y resumen
        total = gastos_mes['Monto'].sum()
        detalles = gastos_mes.groupby('Descripción')['Monto'].sum().to_dict()
        
        # Formatear respuesta
        respuesta = (
            f"📅 *Resumen Mensual* ({inicio_mes.strftime('%d/%m')} - {hoy.strftime('%d/%m')})\n"
            f"🗓️ Mes: {nombre_mes} {hoy.year}\n\n"
            f"💰 *Total gastado:* ${total:.2f}\n\n"
            "🔍 Detalles por categoría:\n"
        )
        
        for categoria, monto in detalles.items():
            respuesta += f"- {categoria}: ${monto:.2f}\n"
        
        await update.message.reply_text(respuesta, parse_mode="Markdown")
        
    except Exception as e:
        logging.error(f"Error en gasto_mensual: {str(e)}")
        await update.message.reply_text("❌ Error al calcular el gasto mensual")


def main():
    application = ApplicationBuilder().token("7686874612:AAEsfK5izb7Y55z_-m5WVB8WBh-JywmN1IU").build()
    application.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio))
    application.add_handler(CommandHandler("eliminaroperacion", eliminar_operacion))
    # Añadir nuevo comando
    application.add_handler(CommandHandler("descargarexcel", descargar_excel))
    # Configurar el ConversationHandler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('registrargasto', registrar_gasto)],
        states={
            ESPERANDO_GASTOS: [MessageHandler(filters.TEXT & ~filters.COMMAND, procesar_gastos_texto)]
        },
        fallbacks=[]
    )
    application.add_handler(CommandHandler("gastosemanal", gasto_semanal))
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("gastomensual", gasto_mensual))
    
    application.run_polling()


if __name__ == "__main__":
    main()