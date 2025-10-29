from cryptography.fernet import Fernet
from services.utils import get_boolean_input
import os

def createEncryptionKey():
    KEY_PATH = "encryption/secret.key"
    
    if os.path.exists(KEY_PATH):
        print(f"{KEY_PATH} already exists. Delete it manually if you want to regenerate the key.")
        exit(1)
    key = Fernet.generate_key()
    with open("encryption/secret.key", "wb") as f:
        f.write(key)

    print("Secret key generated and saved to secret.key")


def encryptPassword():
    ENC_PATH = "encryption/email_password.enc"
    
    # Check for existing encrypted password
    if os.path.exists(ENC_PATH):
        print(f"{ENC_PATH} already exists. Delete it manually if you want to re-encrypt a new password.")
        exit(1)
    # Load your encryption key
    with open("encryption/secret.key", "rb") as f:
        key = f.read()

    fernet = Fernet(key)

    # Encrypt your email password
    raw_password = input("Enter the password you want to encrypt: ").strip()
    encrypted = fernet.encrypt(raw_password.encode())

    with open("encryption/email_password.enc", "wb") as f:
        f.write(encrypted)

    print("Encrypted password saved to email_password.enc")
    
def encryptEtradeKeySecret(sandbox):

    sandbox = get_boolean_input("Run in Sandbox mode?")
    sandbox_suffix = "sandbox" if sandbox else "prod" 
    
    etrade_key = f"encryption/etrade_consumer_key_{sandbox_suffix}.enc"
    skip_key = False
    
    etrade_secret = f"encryption/etrade_consumer_secret_{sandbox_suffix}.enc"
    skip_secret = False 
    # Check for existing encrypted password
    if os.path.exists(etrade_key):
        print(f"{etrade_key} already exists. Delete it manually if you want to re-encrypt a new etrade key.")
        skip_key = True
        
    if os.path.exists(etrade_secret):
        print(f"{etrade_key} already exists. Delete it manually if you want to re-encrypt a new etrade key.")
        skip_key = True
           
        
    # Load your encryption key
    with open("encryption/secret.key", "rb") as f:
        key = f.read()

    fernet = Fernet(key)


    # Encrypt your email password
    raw_etrade_key = input("Enter the Etrade Key you want to encrypt: ").strip()
    encrypted_key = fernet.encrypt(raw_etrade_key.encode())

    with open(etrade_key, "wb") as f:
        f.write(encrypted_key)
        
    raw_etrade_secret = input("Enter the Etrade Secret you want to encrypt: ").strip()
    encrypted_secret = fernet.encrypt(raw_etrade_secret.encode())

    with open(etrade_secret, "wb") as f:
        f.write(encrypted_secret)

    print("Encrypted Etrade Key and Secret")

if __name__ == "__main__":
    encryptEtradeKeySecret(None)
    #encryptPassword()