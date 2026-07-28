# Distributed-Key-Value-Store-Python

## CI/CD setup

This repository uses a GitHub Actions workflow in [.github/workflows/ci-cd.yml](.github/workflows/ci-cd.yml) to:

1. Run a basic CI check on every push and pull request.
2. Deploy to the Ubuntu EC2 instance on pushes to `main`.

### Required GitHub secrets

Add these repository secrets in GitHub:

- `EC2_HOST` - public IP or DNS name of the instance
- `EC2_USER` - usually `ubuntu`
- `EC2_SSH_KEY` - private SSH key that can log into the instance
- `EC2_PORT` - optional, defaults to `22` if you leave it unset in the workflow

### EC2 requirements

- The repo must already exist on the instance at `/home/ubuntu/Distributed-Key-Value-Store-Python`.
- The instance user must be allowed to run `sudo systemctl restart kvstore.service` without a password prompt.
- The security group must allow inbound SSH from GitHub Actions or your admin IP, and app traffic on `8000` if you want direct access.

### Service file

The app is started by [kvstore.service](kvstore.service), which runs `uvicorn main:app --host 0.0.0.0 --port 8000` from the project directory.
"# Distributed-Key-Value-Store-Python" 
