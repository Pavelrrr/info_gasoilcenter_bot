# Telegram Bot for Oil & Gas Well Operations

## Project Overview

This is a Telegram bot built in Python that provides information about oil and gas well operations. The bot allows users to select between different operational modes (drilling and completion) and retrieve detailed information about specific wells, including AI-generated summaries.

## Architecture

### Core Components

1. **`main.py`** - Entry point and webhook handler
   - Handles Yandex Cloud Functions webhook requests
   - Supports both webhook and polling modes for testing
   - Manages bot lifecycle and error handling

2. **`bot.py`** - Main bot logic and handlers
   - Implements Telegram bot using aiogram framework
   - Handles user interactions and command processing
   - Manages inline keyboards and message flow
   - Implements user state management

3. **`services.py`** - Data access layer
   - YDB database integration for well data storage
   - User state management in database
   - Caching mechanisms for performance optimization
   - Temporary file cleanup utilities

4. **`gpt_client.py`** - AI integration
   - YandexGPT integration for generating well operation summaries
   - Asynchronous wrapper for GPT API calls

5. **`utils.py`** - Utility functions
   - HTTP client utilities for file downloads

## Technology Stack

- **Framework**: aiogram 3.13 (Telegram Bot API)
- **Database**: Yandex Database (YDB)
- **AI**: Yandex GPT for text summarization
- **Cloud Platform**: Yandex Cloud Functions
- **Authentication**: Yandex Cloud IAM with OIDC federation
- **Secret Management**: Yandex Lockbox
- **CI/CD**: GitHub Actions

## Features

### User Interface Flow

1. **Start Command** (`/start`)
   - Welcome message with "Start" button
   
2. **Mode Selection**
   - Drilling operations
   - Completion operations
   
3. **Well Selection**
   - Dynamic list of available wells for current date
   - Paginated keyboard layout (3 wells per row)
   
4. **Well Information Display**
   - Detailed well operation descriptions
   - AI-generated summary option
   - Navigation buttons (back to wells, mode selection, start)

### Key Features

- **State Management**: User preferences stored in YDB
- **Message Optimization**: Long messages automatically split
- **Caching**: Well data cached for performance
- **Error Handling**: Comprehensive error logging and user feedback
- **Responsive UI**: Inline keyboards with intuitive navigation

## Database Schema

### `user_state` Table
```sql
CREATE TABLE user_state (
    user_id Uint64,
    mode Utf8,
    message_id Uint64,
    PRIMARY KEY (user_id)
)
```

### `wells` Table
```sql
-- Contains well information filtered by date
-- Fields: well_number, description, date
```

## Configuration

### Environment Variables

#### Required (from Yandex Lockbox)
- `TELEGRAM_TOKEN` - Telegram bot token
- `YDB_ENDPOINT` - YDB connection endpoint
- `YDB_DATABASE` - YDB database path
- `YDB_KEY_SA` - Base64 encoded service account key

#### Required (from GitHub Secrets)
- `FOLDER_ID` - Yandex Cloud folder ID
- `YANDEX_API_KEY` - YandexGPT API key

#### Deployment Secrets
- `YC_SA_ID` - Service account ID for OIDC
- `LOCKBOX_SECRET_ID` - Lockbox secret identifier
- `BUCKET` - S3 bucket for deployment
- `FUNCTION_NAME` - Cloud function name

## Deployment

### CI/CD Pipeline

The project uses GitHub Actions for automated deployment to Yandex Cloud:

1. **Authentication**: OIDC token exchange for IAM authentication
2. **Secret Retrieval**: Fetches configuration from Yandex Lockbox
3. **Function Deployment**: Deploys to Yandex Cloud Functions
4. **Environment Setup**: Configures runtime environment variables

### Deployment Configuration

- **Runtime**: Python 3.11
- **Memory**: 512MB
- **Entry Point**: `main.handler`
- **Architecture**: Serverless (Yandex Cloud Functions)

## Data Flow

1. User sends message to Telegram bot
2. Yandex Cloud Functions receives webhook
3. Bot processes message and updates user state in YDB
4. Well data retrieved from YDB (with caching)
5. Optional AI summary generated via YandexGPT
6. Response sent back to user via Telegram API

## Error Handling

- Database connection retries with multiple query strategies
- Graceful degradation for AI service failures
- Comprehensive logging for debugging
- User-friendly error messages
- Automatic cleanup of temporary resources

## Performance Optimizations

- **Caching**: LRU cache for well descriptions and lists
- **Connection Pooling**: YDB session pooling
- **Message Splitting**: Automatic handling of Telegram message limits
- **Async Operations**: Non-blocking I/O throughout the application

## Security Features

- **IAM Authentication**: Yandex Cloud service account authentication
- **Secret Management**: Sensitive data stored in Yandex Lockbox
- **OIDC Federation**: GitHub Actions authentication via OIDC
- **Input Sanitization**: SQL injection protection in database queries

## Monitoring & Diagnostics

- Structured logging throughout the application
- Diagnostic workflow for Lockbox connectivity testing
- Error tracking with stack traces
- Performance monitoring via Cloud Functions metrics

## Development & Testing

### Local Testing
```bash
python main.py  # Runs in polling mode for local development
```

### Dependencies
All dependencies are managed via `requirements.txt` with specific versions for reproducible builds.

## Future Enhancements

Potential areas for improvement:
- User authentication and authorization
- Advanced well filtering and search
- Historical data access
- Multi-language support
- Real-time notifications for well status changes
- Integration with additional data sources