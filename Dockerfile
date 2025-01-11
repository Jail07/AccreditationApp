FROM ubuntu:22.04

# Установка необходимых зависимостей
ENV DEBIAN_FRONTEND=noninteractive  # Отключение интерактивного ввода
RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    libqt5widgets5 libqt5gui5 libqt5core5a \
    xvfb x11vnc tzdata && \
    rm -rf /var/lib/apt/lists/*

# Создать каталог для XDG_RUNTIME_DIR
RUN mkdir -p /tmp/runtime-root && chmod 700 /tmp/runtime-root

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Установить переменные окружения
ENV DISPLAY=:99
ENV XDG_RUNTIME_DIR=/tmp/runtime-root
ENV TZ=Asia/Bishkek

# Установить и настроить VNC
RUN mkdir -p /root/.vnc && x11vnc -storepasswd AccrApp /root/.vnc/passwd

CMD ["sh", "-c", "rm -f /tmp/.X99-lock && Xvfb :99 -screen 0 1024x768x24 & x11vnc -display :99 -forever -shared -rfbport 5900 -rfbauth /root/.vnc/passwd & python3.11 main.py"]
