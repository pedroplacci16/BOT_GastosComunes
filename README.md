# 1. Instalar dependencias del sistema
sudo apt update && sudo apt install -y ffmpeg python3-pip python3-venv
# Linux (Debian/Ubuntu):
sudo apt-get install ffmpeg

# 2. Instalar dependencias Python (CPU-only)
pip install torch==2.0.1+cpu --index-url https://download.pytorch.org/whl/cpu

pip install python-telegram-bot==20.3 whisper==1.1.10 pandas==2.0.3 openpyxl==3.1.2 python-dotenv==1.0.0
