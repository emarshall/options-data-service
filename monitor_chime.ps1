# Host-side companion to the strategy-monitor Docker service.
#
# The strategy monitor writes one JSON alert per signal day into
# <repo>\runtime\strategy-alerts\ (bind-mounted into the container).
# This script tails that folder and, the instant a NEW alert appears,
#  * plays a chime on this machine's speakers, and
#  * shows a Windows balloon/toast notification.
#
# Run it on the same machine as the Docker stack:
#     powershell -File .\monitor_chime.ps1
# (leave it running; Ctrl+C to stop). For a one-off check of the alert
# handler, drop a file into the alerts dir or use -OneShot.

param(
    [string]$AlertsDir = "",
    [int]$PollSeconds = 3,
    [switch]$OneShot
)

$ErrorActionPreference = "Continue"
if (-not $AlertsDir) {
    $AlertsDir = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "runtime\strategy-alerts"
}

$seen = @{}
$null = New-Item -ItemType Directory -Path $AlertsDir -Force

function Show-Alert($path) {
    $entry = $null
    try { $entry = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json } catch { $entry = $null }
    if ($entry -and $entry.contract_id) {
        $title  = "SPX-CALL ENTRY SIGNAL - $($entry.day)"
        $detail = "BUY $($entry.contract_id) @ ~$($entry.entry_mid) mid (delta $($entry.entry_delta), $($entry.dte) DTE)"
    } else {
        $title  = "SPX-CALL SIGNAL - $((Get-Item -LiteralPath $path).BaseName)"
        $detail = "Trigger detected (no contract selected). See docker logs for details."
    }
    Write-Host ""
    Write-Host "ALERT: $title"
    Write-Host "       $detail"

    # 1) Chime (deterministic - this is the guaranteed path)
    try {
        $wav = Join-Path $env:WINDIR "Media\chimes.wav"
        if (Test-Path -LiteralPath $wav) {
            $player = New-Object System.Media.SoundPlayer $wav
            $player.PlaySync()
        } else {
            [console]::beep(988, 350)
            Start-Sleep -Milliseconds 150
            [console]::beep(1319, 550)
        }
    } catch { Write-Host "  (no system sound available: $_)" }

    # 2) Balloon/toast (best effort - needs an interactive desktop)
    try {
        Add-Type -AssemblyName System.Windows.Forms
        Add-Type -AssemblyName System.Drawing
        $form = New-Object System.Windows.Forms.Form
        $form.ShowInTaskbar = $false
        $form.WindowState = [System.Windows.Forms.FormWindowState]::Minimized
        $icon = New-Object System.Windows.Forms.NotifyIcon
        $icon.Icon = [System.Drawing.SystemIcons]::Warning
        $icon.Visible = $true
        $icon.BalloonTipTitle = $title
        $icon.BalloonTipText = $detail
        $icon.BalloonTipIcon = [System.Windows.Forms.ToolTipIcon]::Warning
        $form.Show()
        $icon.ShowBalloonTip(12000)
        Start-Sleep -Milliseconds 4000
        $icon.Dispose()
        $form.Close()
    } catch { Write-Host "  (balloon not available: $_)" }
}

while ($true) {
    Get-ChildItem -LiteralPath $AlertsDir -Filter *.json |
        Where-Object { -not $seen[$_.FullName] } |
        ForEach-Object {
            $seen[$_.FullName] = $true
            # wait a beat so the file is fully flushed by the container
            Start-Sleep -Milliseconds 500
            Show-Alert $_.FullName
        }
    if ($OneShot) { break }
    Start-Sleep -Seconds $PollSeconds
}