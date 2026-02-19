#!/bin/zsh
set -e

cd "/Users/viditkiyal/Documents/New project/proptech-mvp" || exit 1

if [ -z "$1" ]; then
  echo "Usage: ./push_to_github.command <GITHUB_REPO_URL>"
  echo "Example: ./push_to_github.command https://github.com/yourname/proptech-mvp.git"
  exit 1
fi

REPO_URL="$1"

if [ ! -d .git ]; then
  git init
fi

git add .
if ! git diff --cached --quiet; then
  git commit -m "Deploy-ready Proptech MVP"
fi

git branch -M main

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REPO_URL"
else
  git remote add origin "$REPO_URL"
fi

git push -u origin main

echo "Done: pushed to $REPO_URL"
