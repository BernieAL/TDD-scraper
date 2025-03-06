# Price Tracker Scripts Documentation

## Acknowledgments
### Developer Contributions (Your Work)
- Core application architecture and design
- Price tracking logic and implementation
- Frontend user interface design
- Testing and validation
- Project requirements and specifications

### AI Assistance (Claude - Anthropic)
- AWS infrastructure script generation
- Error handling in deployment scripts
- Documentation structure and organization
- CloudFront distribution configuration
- API Gateway and Lambda integration

## AWS Infrastructure Scripts (`aws/`)

### Core Infrastructure
- `create_core_infra.sh` - Creates base VPC infrastructure
  - Creates VPC, subnet, security group
  - Saves infrastructure IDs to core_infra_config.sh
  - Required before running other infrastructure scripts

### Database Setup
- `setup_dynamodb.sh` - Creates DynamoDB table for app state
  - Creates 'app_status' table for tracking running state
  - Used by Lambda function to check if app is already running

- `setup_users_table.sh` - Creates DynamoDB table for user management
  - Creates 'users' table with email primary key
  - Sets up secondary index for user_id lookups
  - Used for authentication system

### API Setup
- `setup_api.sh` - Creates API Gateway endpoints
  - Creates endpoints: /start-app, /login, /signup
  - Integrates with respective Lambda functions
  - Updates landing-page/config.js with API details

### Cleanup
- `cleanup.sh` - Removes all AWS resources
  - Deletes API Gateway, Lambda functions, DynamoDB tables
  - Removes S3 bucket and CloudFront distribution
  - Cleans up VPC resources and IAM roles
  - Use this to prevent unnecessary AWS charges

## Lambda Functions (`lambda/`)

### Deployment
- `deploy-lambda.sh` - Deploys Lambda functions
  - Packages Lambda code with dependencies
  - Creates/updates Lambda functions on AWS
  - Sets up IAM roles and permissions

### Function Code
- `app_starter/lambda_handler.py` - Main app starter function
  - Checks if app is running
  - Starts ECS tasks if needed
  - Updates app status in DynamoDB

- `auth/lambda_handler.py` - Authentication functions
  - Handles user login
  - Manages user signup
  - Issues JWT tokens

## Landing Page (`landing-page/`)

### Deployment
- `deploy-landing.sh` - Deploys static website
  - Creates S3 bucket
  - Sets up CloudFront distribution
  - Uploads website files

- `deploy-with-invalidation.sh` - Updates website
  - Syncs new files to S3
  - Invalidates CloudFront cache

## Deployment Order
1. `create_core_infra.sh` - Set up base infrastructure
2. `setup_dynamodb.sh` - Create app status table
3. `setup_users_table.sh` - Create users table
4. `deploy-lambda.sh` - Deploy Lambda functions
5. `setup_api.sh` - Create API endpoints
6. `deploy-landing.sh` - Deploy website

## Cleanup
Run `cleanup.sh` to remove all AWS resources when done developing 

# Project Structure
```
project/
├── frontend/         # Frontend static files
│   ├── index.html
│   ├── login.html
│   └── dashboard.html
│   └── backend/
│       ├── lambda/    # Lambda functions
│       ├── aws/      # AWS infrastructure scripts
│       ├── auth/     # Authentication code
│       └── config/   # Configuration files
``` 