An open-source application to monitor and log data from energy devices with multiple protocols. The device data is sent with MQTT or accessible with a REST API.

## Setup

1. Clone the repository.
2. (Optional) Create and activate a virtual environment.
3. Install the project and its dependencies:
   ```bash
   pip install .
   ```
4. Start the application with the provided CLI entry point:
   ```bash
   enervigiledge
   ```

## Configuration

The application expects MQTT credentials provided via environment variables or an `.env` file. A minimal configuration file might look like:

```env
MQTT_CLIENT_ID=example-client
MQTT_ADDRESS=localhost
MQTT_PORT=1883
MQTT_USERNAME=user
MQTT_PASSWORD_ENCRYPTED=...
MQTT_PASSWORD_KEY=...
```

Adjust web server and database options in the source files if necessary.

## Testing

Execute the test suite with:

```bash
pytest
```

or

```bash
python -m pytest
```

## Contributing

Contributions are welcome!

1. Fork the repository and create a new branch for your feature or fix.
2. Ensure code is formatted with `black`.
3. Run `pytest` to verify tests pass.
4. Submit a pull request with a clear description of your changes.
