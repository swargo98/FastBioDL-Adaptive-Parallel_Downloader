#!/usr/bin/env python3
import os
import ftplib
import sys

def ftp_connect(host, username, password, port=21):
    """
    Connect to the FTP server and return an FTP connection object.
    """
    try:
        ftp = ftplib.FTP()
        ftp.connect(host, port)
        ftp.login(username, password)
        print(f"Connected to {host} as {username}")
        return ftp
    except ftplib.all_errors as e:
        print("FTP error:", e)
        sys.exit(1)

def download_file(ftp, remote_file, local_file):
    """
    Download a single file from the FTP server.
    """
    with open(local_file, 'wb') as f:
        def callback(data):
            f.write(data)
        try:
            ftp.retrbinary('RETR ' + remote_file, callback)
            print(f"Downloaded file: {remote_file} -> {local_file}")
        except ftplib.error_perm as e:
            print(f"Failed to download {remote_file}: {e}")

def download_ftp_dir(ftp, remote_dir, local_dir):
    """
    Recursively download a directory from the FTP server.
    """
    # Ensure the local directory exists
    os.makedirs(local_dir, exist_ok=True)
    
    # Change to the remote directory
    try:
        ftp.cwd(remote_dir)
    except ftplib.error_perm as e:
        print(f"Error: cannot access remote directory {remote_dir}: {e}")
        return

    # List items in the remote directory
    try:
        items = ftp.nlst()
    except ftplib.error_perm as e:
        print(f"Error listing directory {remote_dir}: {e}")
        items = []

    for item in items:
        local_path = os.path.join(local_dir, item)
        try:
            # Try to change into the directory: if successful, it's a folder.
            ftp.cwd(item)
            # If we got here, item is a directory; go back one directory and call recursively.
            ftp.cwd('..')
            print(f"Entering directory: {item}")
            download_ftp_dir(ftp, os.path.join(remote_dir, item), local_path)
        except ftplib.error_perm:
            # If changing directory fails, assume it's a file.
            download_file(ftp, item, local_path)

    # Go up to the parent directory on the FTP server before returning.
    try:
        ftp.cwd("..")
    except ftplib.error_perm:
        pass

def main():
    # Get connection details from the user
    host = '131.151.90.131'
    username = 'rms'
    password = '112358'
    remote_dir = '/'
    local_dir = '.'

    ftp = ftp_connect(host, username, password)
    ftp.set_pasv(True)

    # Start downloading
    print(f"Starting download of remote directory '{remote_dir}' to local directory '{local_dir}'")
    download_ftp_dir(ftp, remote_dir, local_dir)

    ftp.quit()
    print("Download completed.")

if __name__ == '__main__':
    main()