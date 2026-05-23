function handler(event) {
  var request = event.request;
  var headers = request.headers;

  // Allow CORS preflight through without an API key. The browser doesn't
  // include custom headers (like X-Api-Key) on the preflight, so checking
  // for the key here would break the preflight handshake.
  if (request.method === 'OPTIONS') {
    return request;
  }

  var apiKey = headers['x-api-key'];
  var expected = '${expected_api_key}';

  if (!apiKey || apiKey.value !== expected) {
    return {
      statusCode: 403,
      statusDescription: 'Forbidden',
      headers: {
        'content-type': { value: 'application/json' }
      },
      body: '{"error":"unauthorized"}'
    };
  }

  return request;
}
