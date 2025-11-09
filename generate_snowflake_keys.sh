#!/bin/bash
set -e

# Script to generate Snowflake RSA key pair for authentication
# This script creates encrypted private/public keys for secure Snowflake connection

echo "=========================================="
echo "Snowflake Key Pair Generation Script"
echo "=========================================="
echo ""

# Create snowflake directory if it doesn't exist
mkdir -p snowflake
cd snowflake

# Check if keys already exist
if [ -f "rsa_key_encrypted.p8" ]; then
    echo "⚠️  Warning: Keys already exist in snowflake/ directory"
    echo "Existing files:"
    ls -la rsa_key*
    echo ""
    read -p "Do you want to overwrite them? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "❌ Aborted. No changes made."
        exit 0
    fi
    echo ""
fi

echo "Step 1: Generating RSA private key (2048-bit)..."
openssl genrsa -out rsa_key.pem 2048 2>/dev/null

echo "Step 2: Encrypting private key with AES-256..."
echo "You will be prompted to enter a password (twice)."
echo "⚠️  IMPORTANT: Remember this password - you'll need it for SNOWFLAKE_PRIVATE_KEY_PASSWORD"
openssl pkcs8 -topk8 -inform PEM -in rsa_key.pem -out rsa_key_encrypted.p8 -v2 aes256

echo ""
echo "Step 3: Setting secure file permissions..."
chmod 600 rsa_key_encrypted.p8
chmod 600 rsa_key.pem

echo "Step 4: Extracting public key..."
openssl rsa -in rsa_key_encrypted.p8 -pubout -out rsa_key_pub.pem 2>/dev/null

echo "Step 5: Preparing public key value for Snowflake..."
grep -v "BEGIN PUBLIC" rsa_key_pub.pem | grep -v "END PUBLIC" | tr -d '\n' > rsa_key_pub_value.txt
echo "" >> rsa_key_pub_value.txt

echo ""
echo "✅ Keys generated successfully!"
echo ""
echo "Files created in snowflake/ directory:"
echo "  - rsa_key.pem (unencrypted private key - for backup only)"
echo "  - rsa_key_encrypted.p8 (encrypted private key - USE THIS)"
echo "  - rsa_key_pub.pem (public key file)"
echo "  - rsa_key_pub_value.txt (public key value for Snowflake)"
echo ""
echo "=========================================="
echo "NEXT STEPS:"
echo "=========================================="
echo ""
echo "1. Copy the public key value:"
echo "   cat snowflake/rsa_key_pub_value.txt"
echo ""
echo "2. Run this SQL in Snowflake (as ACCOUNTADMIN):"
echo "   USE ROLE ACCOUNTADMIN;"
echo "   ALTER USER <your_username> SET RSA_PUBLIC_KEY='<paste_public_key_value>';"
echo ""
echo "3. Update your .env file with:"
echo "   SNOWFLAKE_PRIVATE_KEY_FILE=$(pwd)/rsa_key_encrypted.p8"
echo "   SNOWFLAKE_PRIVATE_KEY_PASSWORD=<the_password_you_just_entered>"
echo ""
echo "4. Test the configuration:"
echo "   evsnow validate-config"
echo ""
echo "=========================================="
echo ""
echo "📋 Your public key value is:"
echo "=========================================="
cat rsa_key_pub_value.txt
echo ""
echo "=========================================="
echo ""
echo "⚠️  SECURITY REMINDERS:"
echo "  - Never commit rsa_key*.pem or rsa_key*.p8 files to git"
echo "  - Store the password in a secure location"
echo "  - These files are already in .gitignore"
echo ""
