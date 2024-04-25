function getGeolocation(callback) {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        position => {
          const location = {
            latitude: position.coords.latitude,
            longitude: position.coords.longitude
          };
          callback(location);
        },
        error => {
          console.error('Geolocation error:', error);
          callback(null); // Handle errors or absence of location data gracefully
        }
      );
    } else {
      console.log('Geolocation is not supported by this browser.');
      callback(null); // Handle cases where Geolocation is not supported
    }
  }
  
  analytics.subscribe("all_events", (event) => {
    console.log(`all events: ${event.name}`, event)
  });
  
  analytics.subscribe("page_viewed", (event) => {
    getGeolocation(location => {
      let sendEventData = {
        event_name: event.name,
        user_id: event.clientId,
        data: event.data,
        context: event.context,
        pageURL: event.context.document.location.href,
        location: location
      };
      console.log(`event: ${event.name} with location`, sendEventData);
    });
  });
  
  analytics.subscribe("product_viewed", (event) => {
    getGeolocation(location => {
      let sendEventData = {
        user_id: event.clientId,
        event_name: event.name,
        pageURL: event.context.document.location.href,
        location: location,
        products: [
          {
            product_id: event.data.productVariant.product.id,
            product_name: event.data.productVariant.product.title,
            currency: event.data.productVariant.price.currencyCode,
            product_price: event.data.productVariant.price.amount,
          },
        ],
      };
      console.log(`event: ${event.name} with location`, sendEventData);
    });
  });
  
  analytics.subscribe("search_submitted", (event) => {
    getGeolocation(location => {
      let sendEventData = {
        user_id: event.clientId,
        event_name: event.name,
        search_string: event.data.query,
        pageURL: event.context.document.location.href,
        location: location
      };
      console.log(`event: ${event.name} with location`, sendEventData);
    });
  });
  
  analytics.subscribe("product_added_to_cart", (event) => {
    getGeolocation(location => {
      let sendEventData = {
        user_id: event.clientId,
        event_name: event.name,
        pageURL: event.context.document.location.href,
        location: location,
        products: [
          {
            product_id: event.data.cartLine.merchandise.product.id,
            variant_id: event.data.cartLine.merchandise.product.variant_id,
            product_name: event.data.cartLine.merchandise.product.title,
            currency: event.data.cartLine.merchandise.price.currencyCode,
            product_price: event.data.cartLine.merchandise.price.amount,
          },
        ],
      };
      console.log(`event: ${event.name} with location`, sendEventData);
    });
  });
  
  analytics.subscribe("checkout_started", (event) => {
    getGeolocation(location => {
      let sendEventData = {
        user_id: event.clientId,
        event_name: event.name,
        products: [],
        currency: event.data.checkout.currencyCode,
        total_price: event.data.checkout.totalPrice.amount,
        pageURL: event.context.document.location.href,
        line_items: event.data.checkout.lineItems,
        location: location
      };
      console.log(`event: ${event.name} with location`, sendEventData);
    });
  });
  
  analytics.subscribe("checkout_completed", (event) => {
    getGeolocation(location => {
      let sendEventData = {
        user_id: event.clientId,
        event_name: event.name,
        products: [],
        currency: event.data.checkout.currencyCode,
        total_price: event.data.checkout.totalPrice.amount,
        pageURL: event.context.document.location.href,
        line_items: event.data.checkout.lineItems,
        location: location
      };
      console.log(`event: ${event.name} with location`, sendEventData);
    });
  });
  