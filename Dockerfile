FROM ubuntu:22.04

# Установка необходимых зависимостей
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    libqt5widgets5 libqt5gui5 libqt5core5a \
    libglu1-mesa libxrender1 libxi6 libxrandr-dev \
    libxcb-xinerama0 \
    libxcb1 \
    libx11-xcb1 \
    libgl1-mesa-glx \
    xorg-dev \
    libxcb-render-util0 \
    libxcb-shm0 \
    xvfb x11vnc tzdata && \
    rm -rf /var/lib/apt/lists/*

# Создать каталог для XDG_RUNTIME_DIR
RUN mkdir -p /tmp/runtime-root && chmod 700 /tmp/runtime-root

# Установить и настроить VNC
RUN mkdir -p /root/.vnc && x11vnc -storepasswd AccrApp /root/.vnc/passwd

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Установить переменные окружения
ENV DISPLAY=:99
ENV QT_QPA_PLATFORM=xcb
ENV QT_PLUGIN_PATH=/usr/lib/x86_64-linux-gnu/qt5/plugins
ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
ENV XDG_RUNTIME_DIR=/tmp/runtime-root
ENV TZ=Asia/Bishkek

CMD ["sh", "-c", "rm -f /tmp/.X99-lock && Xvfb :99 -screen 0 1024x768x24 & x11vnc -display :99 -forever -shared -rfbport 5900 -rfbauth /root/.vnc/passwd & python3 main.py"]
