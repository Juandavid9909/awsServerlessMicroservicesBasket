import { DeleteItemCommand, GetItemCommand, PutItemCommand, ScanCommand } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { PutEventsCommand } from "@aws-sdk/client-eventbridge";

import { ddbClient } from "./ddbClient";
import { ebClient } from "./eventBridgeClient";

exports.handler = async(event) => {
    let body;

    try {
        switch (event.httpMethod) {
            case "GET":
                if (event.pathParameters != null) {
                    body = await getBasket(event.pathParameters.userName);
                } else {
                    body = await getAllBaskets();
                }

                break;

            case "POST":
                if (event.path == "/basket/checkout") {
                    body = await checkoutBasket(event);
                } else {
                    body = await createBasket(event);
                }

                break;

            case "DELETE":
                body = await deleteBasket(event.pathParameters.userName);

                break;

            default:
                throw new Error(`Unsupported route: "${event.httpMethod}"`);
        }

        console.log(body);

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: `Successfully finished operation: "${event.httpMethod}"`,
                body
            })
        };
    } catch (error) {
        console.error(error);

        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Failed to perform operation.",
                errorMsg: error.message,
                errorStack: error.stack
            })
        }
    }
};

const getBasket = async(userName) => {
    try {
        const params = {
            TableName: process.env.DYNAMODB_TABLE_NAME,
            Key: marshall({ userName })
        };

        const { Item } = await ddbClient.send(new GetItemCommand(params));

        console.log(Item);

        return Item ? marshall(Item) : {};
    } catch (error) {
        console.error(error);

        throw error;
    }
};

const getAllBaskets = async() => {
    try {
        const params = {
            TableName: process.env.DYNAMODB_TABLE_NAME
        };

        const { Items } = await ddbClient.send(new ScanCommand(params));

        console.log(Items);

        return Items ? Items.map((item) => unmarshall(item)) : {};
    } catch (error) {
        console.error(error);

        throw error;
    }
};

const createBasket = async(event) => {
    try {
        const requestBody = JSON.parse(event.body);

        const params = {
            TableName: process.env.DYNAMODB_TABLE_NAME,
            Item: marshall(requestBody || {})
        };

        const createResult = await ddbClient.send(new PutItemCommand(params));

        console.log(createResult);

        return createResult;
    } catch (error) {
        console.error(error);

        throw error;
    }
};

const deleteBasket = async(userName) => {
    try {
        const params = {
            TableName: process.env.DYNAMODB_TABLE_NAME,
            Key: marshall({ userName })
        };

        const deleteResult = await ddbClient.send(new DeleteItemCommand(params));

        console.log(deleteResult);

        return deleteResult;
    } catch (error) {
        console.error(error);

        throw error;
    }
};

const checkoutBasket = async(event) => {
    // Publish an event to eventbridge -- this will subscribe by order microservice and start ordering process.
    const checkoutRequest = JSON.parse(event.body);

    if (checkoutRequest == null || checkoutRequest.userName == null) {
        throw new Error(`userName should exist in checkoutRequest: "${checkoutRequest}"`);
    }

    // 1. Get existing basket with items
    const basket = await getBasket(checkoutRequest.userName);

    // 2. Create an event JSON object with basket items, calculate totalprice, prepare order create JSON data to send ordering ms
    let checkoutPayload = prepareOrderPayload(checkoutRequest, basket);

    // 3. Publish an event to eventbridge - this will subscribe by order microservice and start ordering process
    const publishedEvent = await publishCheckoutBasketEvent(checkoutPayload);

    // 4. Remove existing basket
    await deleteBasket(checkoutRequest.userName);
};

const prepareOrderPayload = (checkoutRequest, basket) => {
    try {
        if (basket == null || basket.items == null) {
            throw new Error(`basket should exist in items: "${basket}"`);
        }

        let totalPrice = 0;

        basket.items.forEach((item) => totalPrice = totalPrice + item.price);

        checkoutRequest.totalPrice = totalPrice;

        Object.assign(checkoutRequest, basket);

        console.log("Success prepareOrderPayload, orderPayload:", checkoutRequest);

        return checkoutRequest;
    } catch (error) {
        console.error(error);

        throw error;
    }
};

const publishCheckoutBasketEvent = async(checkoutPayload) => {
    try {
        const params = {
            Entries: [{
                Source: process.env.EVENT_SOURCE,
                Detail: JSON.stringify(checkoutPayload),
                DetailType: process.env.EVENT_DETAILTYPE,
                Resources: [],
                EventBusName: process.env.EVENT_BUSNAME
            }]
        };

        const data = await ebClient.send(new PutEventsCommand(params));

        console.log("Success, ecent sent; requestID:", data);

        return data;
    } catch (error) {
        console.error(error);

        throw error;
    }
};