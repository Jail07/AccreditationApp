from flask import Flask, request, jsonify
import os

UPLOAD_FOLDER = '/app/uploads'  # Папка для хранения загруженных файлов

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Создаем папку, если ее нет
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "Нет файла для загрузки"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Файл не выбран"}), 400
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(file_path)
    return jsonify({"message": "Файл загружен", "path": file_path}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
