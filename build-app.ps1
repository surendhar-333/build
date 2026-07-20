[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$sourcePath = Join-Path $PSScriptRoot "LoadingUiApp.cs"
$launcherPath = Join-Path $PSScriptRoot "run-with-ui.ps1"
$demoPath = Join-Path $PSScriptRoot "demo-interactive-ui.sh"
$outputPath = Join-Path $PSScriptRoot "LoadingUI-Bundled.exe"
$temporaryDirectory = Join-Path ([IO.Path]::GetTempPath()) ("LoadingUI-build-" + [Guid]::NewGuid().ToString("N"))
$temporaryOutput = Join-Path $temporaryDirectory "LoadingUI-Bundled.exe"

$compilerCandidates = @(
    (Join-Path $env:WINDIR "Microsoft.NET\Framework64\v4.0.30319\csc.exe"),
    (Join-Path $env:WINDIR "Microsoft.NET\Framework\v4.0.30319\csc.exe")
)
$compiler = $compilerCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1

if (-not $compiler) {
    throw "The built-in .NET Framework C# compiler was not found."
}

foreach ($requiredPath in @($sourcePath, $launcherPath, $demoPath)) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Required build input not found: $requiredPath"
    }
}

New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
try {
    & $compiler /nologo /target:exe /platform:anycpu `
        "/out:$temporaryOutput" `
        "/resource:$launcherPath,LoadingUI.Resources.run-with-ui.ps1" `
        "/resource:$demoPath,LoadingUI.Resources.demo-interactive-ui.sh" `
        $sourcePath
    if ($LASTEXITCODE -ne 0) {
        throw "C# compilation failed with exit code $LASTEXITCODE."
    }

    Move-Item -LiteralPath $temporaryOutput -Destination $outputPath -Force
}
finally {
    $resolvedTemporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    $resolvedTemporaryDirectory = [IO.Path]::GetFullPath($temporaryDirectory)
    if ($resolvedTemporaryDirectory.StartsWith($resolvedTemporaryRoot) -and (Test-Path -LiteralPath $resolvedTemporaryDirectory)) {
        Remove-Item -LiteralPath $resolvedTemporaryDirectory -Recurse -Force
    }
}

Write-Host "Built: $outputPath" -ForegroundColor Green
