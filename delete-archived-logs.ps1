<#
.SYNOPSIS
    Deletes files from the llm-proxy logs folder that are present in the
    given password-protected 7z archive.

.DESCRIPTION
    1. Reads the archive password from the password file (waits for it to be
       non-empty if the password has not been written yet).
    2. Lists the archive contents with 7-Zip (no extraction).
    3. Maps each archive entry to a file in the logs folder (by relative path
       under a "logs/" prefix, or by file name).
    4. Deletes the matching files (with confirmation, unless -Force).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\delete-archived-logs.ps1 -DryRun
    Preview what would be deleted.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\delete-archived-logs.ps1
    Do the deletion.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\delete-archived-logs.ps1 -Force
    Delete without the interactive confirmation.
#>
[CmdletBinding()]
param(
    [string]$Archive      = 'b:\intersub\temp\llm-proxy-logs-261103.7z',
    [string]$LogsDir      = 'B:\intersub\temp\llm-proxy\logs',
    [string]$PasswordFile = 'B:\intersub\temp\llm-proxy-logs-password.txt',
    [string]$SevenZip     = 'C:\Program Files\7-Zip\7z.exe',
    [int]$WaitForPasswordSeconds = 600,
    # Only files whose name ends with one of these suffixes are deleted.
    # Pass an empty array to delete any archived file.
    [string[]]$OnlyExtensions = @('.req.json', '.resp.json'),
    [switch]$DryRun,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# --- Sanity checks -----------------------------------------------------------
foreach ($t in @(
    @{ N = '7-Zip';       P = $SevenZip },
    @{ N = 'Archive';     P = $Archive },
    @{ N = 'Logs folder'; P = $LogsDir }
)) {
    if (-not (Test-Path -LiteralPath $t.P)) {
        throw "$($t.N) not found: $($t.P)"
    }
}

# --- Read the password (wait until the file is non-empty) --------------------
function Get-ArchivePassword {
    $deadline = (Get-Date).AddSeconds($WaitForPasswordSeconds)
    while ($true) {
        if (Test-Path -LiteralPath $PasswordFile) {
            $raw = Get-Content -LiteralPath $PasswordFile -Raw
            if ($null -ne $raw) {
                if ($raw.Length -gt 0 -and $raw[0] -eq [char]0xFEFF) { $raw = $raw.Substring(1) }
                $pw = $raw.Trim("`r", "`n", "`t", ' ')
                if ($pw) { return $pw }
            }
        }
        if ((Get-Date) -ge $deadline) {
            throw "No password found in '$PasswordFile' after $WaitForPasswordSeconds s."
        }
        Write-Host "Password file is empty/not ready yet, waiting... (Ctrl+C to abort)"
        Start-Sleep -Seconds 2
    }
}

$password = Get-ArchivePassword

# --- List archive contents (no extraction) -----------------------------------
Write-Host "Listing archive: $Archive"
$listing = & $SevenZip l -slt -bso0 -bse0 -y "-p$password" $Archive 2>&1
$exit = $LASTEXITCODE
if ($exit -ne 0) {
    if ($exit -eq 255) { throw "7-Zip failed (exit 255): wrong password or corrupted archive." }
    throw "7-Zip failed with exit code $exit. Output: $listing"
}

# Parse the -slt (technical) listing into file entries.
# Per-entry blocks have no "Type = File" line (that only exists in the
# header), so files are identified by "Attributes" NOT starting with D
# (directories are "D", files are "A"). Entry blocks start after the
# first "----------" separator line.
$entries = New-Object System.Collections.Generic.List[string]
$inEntries = $false
$curPath = $null
$curAttr = $null
foreach ($line in $listing) {
    if ($line -match '^-{10,}\s*$') {
        # separator: flush previous block, switch into entry section
        if ($curPath -and $curAttr -and $curAttr -notmatch '^D') { $entries.Add($curPath) }
        $curPath = $null; $curAttr = $null
        $inEntries = $true
        continue
    }
    if (-not $inEntries) { continue }
    if ($line -match '^Path = (.*)$') {
        $curPath = $Matches[1]
    }
    elseif ($line -match '^Attributes = (.*)$') {
        $curAttr = $Matches[1].Trim()
    }
    elseif ($line -match '^\s*$') {
        if ($curPath -and $curAttr -and $curAttr -notmatch '^D') { $entries.Add($curPath) }
        $curPath = $null; $curAttr = $null
    }
}
if ($curPath -and $curAttr -and $curAttr -notmatch '^D') { $entries.Add($curPath) }

if ($entries.Count -eq 0) {
    Write-Warning 'No file entries found in the archive. Nothing to do.'
    exit 0
}
Write-Host "Archive contains $($entries.Count) file entry(ies)."

# --- Map archive entries to files on disk ------------------------------------
# Index every file currently in the logs folder (recursive) by file name.
$byName = @{}
Get-ChildItem -LiteralPath $LogsDir -File -Recurse | ForEach-Object {
    $byName[$_.Name] = $_.FullName
}

$toDelete = New-Object System.Collections.Generic.List[string]
$seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$notFound = 0

foreach ($entry in $entries) {
    # Keep only files matching the requested extension suffixes.
    if ($OnlyExtensions.Count -gt 0) {
        $entryName = Split-Path -Path ($entry -replace '\\', '\') -Leaf
        $keep = $false
        foreach ($ext in $OnlyExtensions) {
            if ($entryName -like "*$ext") { $keep = $true; break }
        }
        if (-not $keep) { continue }
    }

    $norm = $entry -replace '\\', '/'
    $candidates = @()

    # Full relative path under a "logs/" prefix, if present (e.g. logs/sub/a.log).
    if ($norm -match '(^|.*/)logs/(.+)$') {
        $rel = $Matches[2] -replace '/', '\'
        $candidates += (Join-Path $LogsDir $rel)
    }
    # Otherwise / additionally match by plain file name.
    $name = Split-Path -Path ($norm -replace '/', '\') -Leaf
    if ($name -and $byName.ContainsKey($name)) {
        $candidates += $byName[$name]
    }

    $found = $false
    foreach ($cand in $candidates) {
        if (Test-Path -LiteralPath $cand -PathType Leaf) {
            $resolved = (Resolve-Path -LiteralPath $cand).Path
            if ($seen.Add($resolved)) { $toDelete.Add($resolved) }
            $found = $true
            break
        }
    }
    if (-not $found) { $notFound++ }
}

Write-Host "Matched on disk: $($toDelete.Count)  (archive entries not found in logs folder: $notFound)"

if ($toDelete.Count -eq 0) {
    Write-Host 'No files to delete.'
    exit 0
}

# --- Delete -------------------------------------------------------------------
if ($DryRun) {
    Write-Host "`n[DRY RUN] Would delete the following files:"
    $toDelete | ForEach-Object { Write-Host "  $_" }
    exit 0
}

if (-not $Force) {
    $answer = Read-Host "`nDelete $($toDelete.Count) file(s) from '$LogsDir'? [y/N]"
    if ($answer -notmatch '^(y|yes)$') {
        Write-Host 'Aborted, nothing deleted.'
        exit 0
    }
}

$deleted = 0
$failed = @()
foreach ($f in $toDelete) {
    try {
        Remove-Item -LiteralPath $f -Force
        $deleted++
    } catch {
        $failed += "$f : $($_.Exception.Message)"
    }
}

Write-Host "Deleted $deleted / $($toDelete.Count) file(s)."
if ($failed.Count -gt 0) {
    Write-Warning "Failed to delete $($failed.Count) file(s):"
    $failed | ForEach-Object { Write-Warning "  $_" }
    exit 1
}
exit 0
