# Installs or updates Herald for the current user from the latest GitHub release.
#   irm https://raw.githubusercontent.com/freesaladman/Herald/main/scripts/install.ps1 | iex
# Set $env:HERALD_VERSION (e.g. "v0.1.0") first to install a specific release.
& {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    $repo = 'freesaladman/Herald'

    if (-not [Environment]::Is64BitOperatingSystem) { throw 'Herald needs 64-bit Windows.' }

    $api = if ($env:HERALD_VERSION) {
        "https://api.github.com/repos/$repo/releases/tags/$env:HERALD_VERSION"
    } else {
        "https://api.github.com/repos/$repo/releases/latest"
    }
    $release = Invoke-RestMethod -Uri $api -Headers @{ 'User-Agent' = 'herald-install' }
    $asset = $release.assets | Where-Object { $_.name -like '*_x64-setup.exe' } | Select-Object -First 1
    if (-not $asset) { throw "Release $($release.tag_name) has no Windows installer." }
    $sum = $release.assets | Where-Object { $_.name -eq "$($asset.name).sha256" } | Select-Object -First 1
    if (-not $sum) { throw "Release $($release.tag_name) has no checksum for $($asset.name)." }

    $dir = Join-Path ([IO.Path]::GetTempPath()) "herald-install-$([guid]::NewGuid().ToString('N'))"
    New-Item -ItemType Directory -Path $dir | Out-Null
    try {
        $exe = Join-Path $dir $asset.name
        Write-Host "Downloading Herald $($release.tag_name)..."
        Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $exe -UseBasicParsing
        $expected = ((Invoke-RestMethod -Uri $sum.browser_download_url).Trim() -split '\s+')[0]
        $actual = (Get-FileHash -Path $exe -Algorithm SHA256).Hash
        if ($actual -ine $expected) { throw "Checksum mismatch for $($asset.name)." }

        # Silent mode closes a running Herald before replacing it.
        Write-Host 'Installing...'
        $p = Start-Process -FilePath $exe -ArgumentList '/S' -Wait -PassThru
        if ($p.ExitCode -ne 0) { throw "The installer exited with code $($p.ExitCode)." }
    } finally {
        Remove-Item -Recurse -Force -Path $dir -ErrorAction SilentlyContinue
    }

    $app = Join-Path $env:LOCALAPPDATA 'Herald\herald-app.exe'
    if (Test-Path $app) {
        Start-Process -FilePath $app
        Write-Host 'Herald is installed and running in the tray. Open a new terminal to use the herald CLI.'
    } else {
        Write-Host 'Herald is installed. Start it from the Start menu; open a new terminal to use the herald CLI.'
    }
}
