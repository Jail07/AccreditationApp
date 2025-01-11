FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Установка необходимых инструментов для сборки Qt
RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    python3.11 python3.11-distutils python3.11-venv \
    wget tar git perl g++ make \
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

# Копируем архив Qt в контейнер
COPY tqtc-qt5-everywhere-opensource-src-5.15.3.tar.xz /root/

# Установка Qt 5.15.3
RUN cd /root && \
    tar -xf tqtc-qt5-everywhere-opensource-src-5.15.3.tar.xz && \
    cd tqtc-qt5-everywhere-src-5.15.3 && \
    perl init-repository && \
    ./configure -prefix /usr/local/qt5 && \
    make -j$(nproc) && \
    make install && \
    rm -rf /root/tqtc-qt5-everywhere-src-5.15.3 /root/tqtc-qt5-everywhere-opensource-src-5.15.3.tar.xz


# Создать каталог для XDG_RUNTIME_DIR
RUN mkdir -p /tmp/runtime-root && chmod 700 /tmp/runtime-root

# Установить и настроить VNC
RUN mkdir -p /root/.vnc && x11vnc -storepasswd AccrApp /root/.vnc/passwd

# Установить переменные окружения
ENV PATH="/usr/local/qt5/bin:$PATH"
ENV DISPLAY=:99
ENV QT_QPA_PLATFORM=xcb
ENV QT_PLUGIN_PATH=/usr/local/Qt-5.15.3/plugins
ENV LD_LIBRARY_PATH=/usr/local/Qt-5.15.3/lib
ENV XDG_RUNTIME_DIR=/tmp/runtime-root
ENV TZ=Asia/Bishkek

WORKDIR /app

COPY . .

# Установка Python-зависимостей
RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["sh", "-c", "rm -f /tmp/.X99-lock && Xvfb :99 -screen 0 1024x768x24 & x11vnc -display :99 -forever -shared -rfbport 5900 -rfbauth /root/.vnc/passwd & python3.11 main.py"]
