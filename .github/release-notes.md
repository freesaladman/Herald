## Install

**Windows**: run in PowerShell

```powershell
irm https://raw.githubusercontent.com/freesaladman/Herald/main/scripts/install.ps1 | iex
```

or download `Herald_<version>_x64-setup.exe` below. The installer is not code-signed, so SmartScreen may say "Windows protected your PC": choose **More info**, then **Run anyway**.

**macOS** (not verified: built by CI but not yet tested on a Mac): run in Terminal

```sh
curl -fsSL https://raw.githubusercontent.com/freesaladman/Herald/main/scripts/install.sh | sh
```

or download the `.dmg` for your Mac (`aarch64` for Apple Silicon, `x64` for Intel). The app is not notarized, so macOS blocks a downloaded copy on first open: go to **System Settings > Privacy & Security** and choose **Open Anyway**, or run `xattr -dr com.apple.quarantine /Applications/Herald.app`. The install script does not need this.

Each file has a matching `.sha256`.
