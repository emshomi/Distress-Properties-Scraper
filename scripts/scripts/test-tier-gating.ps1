<#
================================================================================
 Govire - Tier-Gating Regression Test  (Windows PowerShell 5.1 compatible)
================================================================================
 Proves the BACKEND enforces tier gating, server-side, non-bypassable.
 The browser only shows the cosmetic frontend lock; this hits the real API
 with the three QA access keys and asserts each tier behaves correctly.

 WHY THIS EXISTS:
   A stale Railway deploy once shipped with the gating code un-deployed. The
   frontend still greyed the controls (cosmetic), so it LOOKED fine while the
   API was wide open. Only a direct API test catches that. Run before every
   deploy. Exits non-zero on any failure so CI can block the deploy.

 USAGE (Windows PowerShell 5.1):
   .\test-tier-gating-ps5.ps1
   .\test-tier-gating-ps5.ps1 -BaseUrl "https://api.govire.com"

 If you get an execution-policy error, run this once in the same window first:
   Set-ExecutionPolicy -Scope Process -Bypass

 NOTE ON AUTH:
   Tier resolves from X-Access-Key (these QA keys live in
   access.access_requests, status=approved). A JWT would override an access
   key, so this script sends NO Authorization header - the key is the tier.
   It does not matter whether your browser is logged in.
================================================================================
#>

[CmdletBinding()]
param(
    [string]$BaseUrl = "https://api.govire.com"
)

$ErrorActionPreference = "Stop"

# TLS 1.2 - older PS 5.1 defaults can fail HTTPS handshakes without this.
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

# --- QA access keys (from access.access_requests) -----------------------------
$Keys = @{
    basic    = "qakey-basic-7Fkq9Wmz2RvT"
    standard = "qakey-standard-3Hnp6Ycx8Bge"
    premium  = "qakey-premium-5Zdw4Qjr1Lst"
}

# --- Test bookkeeping ---------------------------------------------------------
$script:Pass = 0
$script:Fail = 0
$script:Failures = @()

function Assert([bool]$Condition, [string]$Name, [string]$Detail = "") {
    if ($Condition) {
        $script:Pass++
        Write-Host ("  [PASS] " + $Name) -ForegroundColor Green
    } else {
        $script:Fail++
        $script:Failures += $Name
        Write-Host ("  [FAIL] " + $Name) -ForegroundColor Red
        if ($Detail) { Write-Host ("         -> " + $Detail) -ForegroundColor DarkYellow }
    }
}

# Call the API. Returns a PSObject with .Status (int) and .Data (parsed body).
# PS 5.1 THROWS on non-2xx, so we catch and recover the status + body from the
# WebException response stream. Never throws back to the caller - we assert on
# the status code (needed for the 402 owner-gate checks).
function Invoke-Api {
    param(
        [string]$Path,
        [hashtable]$Query = @{},
        [string]$AccessKey = $null
    )

    $qs = ($Query.GetEnumerator() | ForEach-Object {
        [string]$k = $_.Key
        [string]$v = [string]$_.Value
        "$([uri]::EscapeDataString($k))=$([uri]::EscapeDataString($v))"
    }) -join "&"

    $url = "$BaseUrl$Path"
    if ($qs) { $url = "$url`?$qs" }

    $headers = @{ "Accept" = "application/json" }
    if ($AccessKey) { $headers["X-Access-Key"] = $AccessKey }

    try {
        $resp = Invoke-WebRequest -Uri $url -Headers $headers -Method GET -UseBasicParsing
        $status = [int]$resp.StatusCode
        $content = $resp.Content
    } catch [System.Net.WebException] {
        $webResp = $_.Exception.Response
        if ($webResp -ne $null) {
            $status = [int]$webResp.StatusCode
            try {
                $sr = New-Object System.IO.StreamReader($webResp.GetResponseStream())
                $content = $sr.ReadToEnd()
                $sr.Close()
            } catch { $content = "" }
        } else {
            # No HTTP response at all - DNS / TLS / connection failure.
            return [PSCustomObject]@{ Status = -1; Data = $null; Raw = $_.Exception.Message }
        }
    } catch {
        return [PSCustomObject]@{ Status = -1; Data = $null; Raw = $_.Exception.Message }
    }

    $body = $null
    if ($content) {
        try { $body = $content | ConvertFrom-Json } catch { $body = $null }
    }
    return [PSCustomObject]@{ Status = $status; Data = $body; Raw = $content }
}

# Pull the property list out of the success envelope:
#   { success: true, data: { properties: [...], total: n } }
function Get-Properties($resp) {
    if ($null -eq $resp.Data) { return @() }
    if ($null -eq $resp.Data.data) { return @() }
    $props = $resp.Data.data.properties
    if ($null -eq $props) { return @() }
    return @($props)
}

# Is this list sorted by equity (market_value - amount), descending?
# Only meaningful when fields are unlocked (premium). Tolerates nulls at the
# tail (NULLS LAST style) and needs >=2 comparable rows to judge.
function Test-EquityDescending($props) {
    $eq = @()
    foreach ($p in $props) {
        $mv = $p.market_value
        $am = $p.amount
        if ($null -ne $mv -and $null -ne $am) {
            $eq += [double]$mv - [double]$am
        }
    }
    if ($eq.Count -lt 2) { return $null }  # not enough data to judge
    for ($i = 1; $i -lt $eq.Count; $i++) {
        if ($eq[$i] -gt $eq[$i-1]) { return $false }
    }
    return $true
}

Write-Host ""
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host " Govire Tier-Gating Regression Test (PS 5.1)" -ForegroundColor Cyan
Write-Host " Target: $BaseUrl" -ForegroundColor Cyan
Write-Host "==============================================================" -ForegroundColor Cyan

# --- 0. Sanity: API reachable ------------------------------------------------
Write-Host ""
Write-Host "[0] API reachability" -ForegroundColor White
$ping = Invoke-Api -Path "/properties" -Query @{ limit = "1" }
Assert ($ping.Status -eq 200) "API responds 200 on /properties" "Got status $($ping.Status). Raw: $($ping.Raw)"
if ($ping.Status -ne 200) {
    Write-Host ""
    Write-Host "ABORTING: API not reachable. Check Railway deploy / BaseUrl." -ForegroundColor Red
    exit 1
}

# --- 1. ANONYMOUS (no key) -> redaction holds ---------------------------------
Write-Host ""
Write-Host "[1] Anonymous (free) - redaction" -ForegroundColor White
$anon = Invoke-Api -Path "/properties" -Query @{ limit = "10" }
$anonProps = Get-Properties $anon
Assert ($anonProps.Count -gt 0) "Anonymous gets rows back (locked, not empty)" "Got $($anonProps.Count) rows"
if ($anonProps.Count -gt 0) {
    $allAddrNull  = -not ($anonProps | Where-Object { $null -ne $_.address })
    $allOwnerNull = -not ($anonProps | Where-Object { $null -ne $_.owner })
    $allMvNull    = -not ($anonProps | Where-Object { $null -ne $_.market_value })
    Assert $allAddrNull  "Anonymous: address is null on every row"      "Some rows leaked an address"
    Assert $allOwnerNull "Anonymous: owner is null on every row"        "Some rows leaked an owner"
    Assert $allMvNull    "Anonymous: market_value is null on every row" "Some rows leaked market_value"
    $hasLockFlag = [bool]($anonProps | Where-Object { $_.address_locked -eq $true })
    Assert $hasLockFlag "Anonymous: locked fields flagged *_locked=true" "No *_locked flags present"
}

# --- 2. BASIC -> values unlocked, locators still locked -----------------------
Write-Host ""
Write-Host "[2] Basic - value unlocked, locators locked" -ForegroundColor White
$basic = Invoke-Api -Path "/properties" -Query @{ limit = "25" } -AccessKey $Keys.basic
$basicProps = Get-Properties $basic
Assert ($basicProps.Count -gt 0) "Basic gets rows" "Got $($basicProps.Count) rows"
if ($basicProps.Count -gt 0) {
    $anyMv = [bool]($basicProps | Where-Object { $null -ne $_.market_value })
    Assert $anyMv "Basic: market_value is populated (value unlocked)" "No market_value on any row"
    $allAddrNull = -not ($basicProps | Where-Object { $null -ne $_.address })
    Assert $allAddrNull "Basic: address still null (locator locked)" "Basic leaked an address"
}

# --- 3. STANDARD -> full data readable, but filters/sorts BLOCKED -------------
Write-Host ""
Write-Host "[3] Standard - full data, but cannot hunt" -ForegroundColor White

# 3a. Full data readable
$std = Invoke-Api -Path "/properties" -Query @{ limit = "50" } -AccessKey $Keys.standard
$stdProps = Get-Properties $std
$anyAddr = [bool]($stdProps | Where-Object { $null -ne $_.address })
Assert $anyAddr "Standard: address is populated (full read)" "Standard saw no addresses"

# 3b. equity sort must be IGNORED (forced back to default event_date order)
$stdSort = Invoke-Api -Path "/properties" -Query @{ sort = "equity"; order = "desc"; limit = "50" } -AccessKey $Keys.standard
$stdSortProps = Get-Properties $stdSort
$stdIsEquitySorted = Test-EquityDescending $stdSortProps
if ($null -eq $stdIsEquitySorted) {
    Write-Host "  [WARN] Standard equity-sort: too few comparable rows to judge ordering" -ForegroundColor Yellow
} else {
    Assert (-not $stdIsEquitySorted) "Standard: equity sort is IGNORED (not equity-ordered)" `
        "Result came back equity-ordered - sort gate NOT enforced"
}

# 3c. price_min filter must be IGNORED (rows below the threshold still returned)
$threshold = 5000000  # $5M - almost everything is below this
$stdFilter = Invoke-Api -Path "/properties" -Query @{ price_min = "$threshold"; limit = "50" } -AccessKey $Keys.standard
$stdFilterProps = Get-Properties $stdFilter
$belowThreshold = @($stdFilterProps | Where-Object {
    $null -ne $_.market_value -and [double]$_.market_value -lt $threshold
})
Assert ($belowThreshold.Count -gt 0) "Standard: price_min filter is IGNORED (rows below threshold returned)" `
    "No sub-threshold rows - filter may be enforced (gate leaking power to Standard)"

# 3d. /owners must be 402 (premium-only)
$stdOwners = Invoke-Api -Path "/owners" -Query @{ min_parcels = "2" } -AccessKey $Keys.standard
Assert ($stdOwners.Status -eq 402) "Standard: /owners returns HTTP 402 (premium-only)" "Got status $($stdOwners.Status)"

# --- 4. PREMIUM -> full data + full hunt (not over-blocked) -------------------
Write-Host ""
Write-Host "[4] Premium - full data + hunting works" -ForegroundColor White

# 4a. full data
$prem = Invoke-Api -Path "/properties" -Query @{ limit = "50" } -AccessKey $Keys.premium
$premProps = Get-Properties $prem
$anyAddrP = [bool]($premProps | Where-Object { $null -ne $_.address })
$anyMvP   = [bool]($premProps | Where-Object { $null -ne $_.market_value })
Assert $anyAddrP "Premium: address populated" "Premium saw no addresses"
Assert $anyMvP   "Premium: market_value populated" "Premium saw no market_value"

# 4b. equity sort must ACTUALLY sort (gate not over-blocking premium)
$premSort = Invoke-Api -Path "/properties" -Query @{ sort = "equity"; order = "desc"; limit = "50" } -AccessKey $Keys.premium
$premSortProps = Get-Properties $premSort
$premIsEquitySorted = Test-EquityDescending $premSortProps
if ($null -eq $premIsEquitySorted) {
    Write-Host "  [WARN] Premium equity-sort: too few comparable rows to judge ordering" -ForegroundColor Yellow
} else {
    Assert $premIsEquitySorted "Premium: equity sort IS applied (descending)" `
        "Premium equity sort not honored - gate over-blocking premium"
}

# 4c. /owners must be 200 for premium
$premOwners = Invoke-Api -Path "/owners" -Query @{ min_parcels = "2" } -AccessKey $Keys.premium
Assert ($premOwners.Status -eq 200) "Premium: /owners returns HTTP 200" "Got status $($premOwners.Status)"

# --- Summary -----------------------------------------------------------------
Write-Host ""
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host (" RESULT: {0} passed, {1} failed" -f $script:Pass, $script:Fail) -ForegroundColor Cyan
Write-Host "==============================================================" -ForegroundColor Cyan
if ($script:Fail -gt 0) {
    Write-Host ""
    Write-Host "FAILED CHECKS:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host ("  - " + $_) -ForegroundColor Red }
    Write-Host ""
    Write-Host "A failure here means the gate is NOT enforced server-side." -ForegroundColor Red
    Write-Host "Most likely cause: stale Railway deploy. Redeploy and re-run." -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "All tier gates enforced server-side. Safe to deploy." -ForegroundColor Green
exit 0
