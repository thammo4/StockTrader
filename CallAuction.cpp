#include <iostream>
#include <vector>
#include <algorithm>

struct Order {
	bool isBuy;
	double price;
	int quantity;
};


class CallAuction {
private:
	std::vector<Order> orders;

public:
	void addOrder (bool isBuy, double price, int quantity) {
		Order order = {isBuy, price, quantity};
		orders.push_back(order);
	}


	void clearAuction () {
		std::vector<Order> buyOrders;
		std::vector<Order> sellOrders;

		for (const auto& order : orders) {
			if (order.isBuy) 	{ buyOrders.push_back(order); }
			else 				{ sellOrders.push_back(order); }
		}
	}
	std::sort(buyOrders.begin(), buyOrders.end(), [](const Order& a, const Order& b) { return a.price > b.price; });
	std::sort(sellOrders.begin(), sellOrders.end(), [](const Order& a, const Order& b) { return a.price < b.price; });


	int i = 0, j = 0;
	double clearingPrice = 0;
	int clearingQuantity = 0;

	while (i<buyOrders.size() && j<sellOrders.size()) {
		if (buyOrders[i].price >= sellOrders[j].price) {
			int tradeQuantity = std::min(buyOrders[i].quantity, sellOrders[j].quantity);
			clearingPrice = sellOrders[j].price;
			clearingQuantity += tradeQuantity;

			buyOrders[i].quantity -= tradeQuantity;
			sellOrders[j].quantity -= tradeQuantity;

			if (buyOrders[i].quantity == 0) { i++; }
			if (sellOrders[j].quantity == 0) {j++; }
		} else {
			break;
		}
	}

	std::cout << "Clear Price: " << clearingPrice << std::endl;
	std::cout << "Clear Quantity: " << clearingQuantity << std::endl;
}