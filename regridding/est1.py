ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Load the private key for key-based authentication
ssh_private_key_path = "/Users/kmredilla/.ssh/id_rsa"
private_key = paramiko.RSAKey(filename=ssh_private_key_path)

# Connect to the SSH server using key-based authentication
ssh_host = "chinook04.rcs.alaska.edu"
ssh_port = 22
ssh_username = "kmredilla"
ssh.connect(ssh_host, ssh_port, ssh_username, pkey=private_key)


stdin_, stdout, stderr = ssh.exec_command("conda --version")


def command_test(command):
    stdin_, stdout, stderr = ssh.exec_command(command)
    print(stdout.read().decode("utf8").strip())
    print(stderr.read().decode("utf8").strip())
