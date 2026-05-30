function handler(event) {
  var request = event.request;
  var headers = request.headers;

  var auth = headers.authorization;
  var expected = 'Basic ${expected_credentials}';

  if (!auth || auth.value !== expected) {
    return {
      statusCode: 401,
      statusDescription: 'Unauthorized',
      headers: {
        'www-authenticate': { value: 'Basic realm="${realm}"' }
      }
    };
  }

  return request;
}
