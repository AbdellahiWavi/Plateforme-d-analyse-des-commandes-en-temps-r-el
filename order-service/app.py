from flask import Flask, jsonify
import requests

app = Flask(__name__)

orders = {
    1: {
        "order_id": 1,
        "customer_id": 1,
        "product": "Laptop"
    },
    2: {
        "order_id": 2,
        "customer_id": 2,
        "product": "Keyboard"
    }
}

@app.route('/orders/<int:order_id>', methods=['GET'])
def get_order(order_id):

    order = orders.get(order_id)

    if not order:
        return jsonify({"message": "Order not found"}), 404

    customer_response = requests.get(
        f"http://customer-service:5000/customers/{order['customer_id']}"
    )

    customer = customer_response.json()

    return jsonify({
        "order_id": order["order_id"],
        "product": order["product"],
        "customer": customer
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)