#!/bin/bash
set -e  # Exit on error

AWS_REGION="us-east-1"

echo "Creating core infrastructure..."

# Create VPC
echo "Creating VPC..."
VPC_ID=$(aws ec2 create-vpc \
    --cidr-block 10.0.0.0/16 \
    --region $AWS_REGION \
    --query 'Vpc.VpcId' \
    --output text)

# Add name tag to VPC
aws ec2 create-tags \
    --resources $VPC_ID \
    --region $AWS_REGION \
    --tags Key=Name,Value=price-tracker-vpc

# Enable DNS hostnames for VPC
aws ec2 modify-vpc-attribute \
    --vpc-id $VPC_ID \
    --region $AWS_REGION \
    --enable-dns-hostnames "{\"Value\":true}"

# Create subnet
echo "Creating subnet..."
SUBNET_ID=$(aws ec2 create-subnet \
    --vpc-id $VPC_ID \
    --cidr-block 10.0.1.0/24 \
    --region $AWS_REGION \
    --query 'Subnet.SubnetId' \
    --output text)

# Add name tag to subnet
aws ec2 create-tags \
    --resources $SUBNET_ID \
    --region $AWS_REGION \
    --tags Key=Name,Value=price-tracker-subnet

# Create security group
echo "Creating security group..."
SECURITY_GROUP_ID=$(aws ec2 create-security-group \
    --group-name price-tracker-sg \
    --description "Security group for Price Tracker ECS tasks" \
    --vpc-id $VPC_ID \
    --region $AWS_REGION \
    --query 'GroupId' \
    --output text)

# Add name tag to security group
aws ec2 create-tags \
    --resources $SECURITY_GROUP_ID \
    --region $AWS_REGION \
    --tags Key=Name,Value=price-tracker-sg

# Add inbound rules to security group
aws ec2 authorize-security-group-ingress \
    --group-id $SECURITY_GROUP_ID \
    --protocol tcp \
    --port 80 \
    --region $AWS_REGION \
    --cidr 0.0.0.0/0

# Save IDs to a config file
echo "Saving infrastructure IDs..."
cat > aws/core_infra_config.sh << EOF
#!/bin/bash
export VPC_ID=$VPC_ID
export SUBNET_ID=$SUBNET_ID
export SECURITY_GROUP_ID=$SECURITY_GROUP_ID
EOF

echo "Core infrastructure created successfully!"
echo "VPC ID: $VPC_ID"
echo "Subnet ID: $SUBNET_ID"
echo "Security Group ID: $SECURITY_GROUP_ID" 