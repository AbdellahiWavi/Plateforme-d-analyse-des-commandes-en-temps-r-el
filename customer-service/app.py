from flask import Flask, jsonify

app = Flask(__name__)

customers = {
    1: {
        "id": 1,
        "name": "Ahmed",
        "email": "ahmed@gmail.com"
    },
    2: {
        "id": 2,
        "name": "Salah",
        "email": "salah@gmail.com"
    }
}

@app.route("/")
def home():
    return jsonify({
        "service": "Customer Service",
        "status": "running"
    })

@app.route("/customers/<int:id>")
def get_customer(id):
    customer = customers.get(id)

    if customer:
        return jsonify(customer)

    return jsonify({
        "error": "Customer not found"
    }), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)