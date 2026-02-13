param(
    [ValidateSet("smoke", "full")]
    [string]$Mode = "full",
    [string]$PythonBin = "python"
)

$ErrorActionPreference = "Stop"
$outCsv = "results/raw/raw_results.csv"

New-Item -ItemType Directory -Force -Path "results/raw" | Out-Null

$oldErrorAction = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& $PythonBin -c "import ray" 1>$null 2>$null
$rayExitCode = $LASTEXITCODE
$ErrorActionPreference = $oldErrorAction

if ($rayExitCode -ne 0) {
    Write-Host "[WARN] ray is not available. 'ray' mode will run with local process fallback."
}

if ($Mode -eq "smoke") {
    $taskPoints = @(10, 50)
    $repeats = 2
    $units = 5000
}
else {
    $taskPoints = @(10, 20, 50, 100, 200, 500, 1000)
    $repeats = 5
    $units = 20000
}

if (Test-Path $outCsv) {
    Remove-Item $outCsv -Force
}

foreach ($nTasks in $taskPoints) {
    for ($repeatId = 1; $repeatId -le $repeats; $repeatId++) {
        $seed = 42 + $repeatId

        Write-Host "[RUN] baseline n_tasks=$nTasks repeat=$repeatId"
        & $PythonBin src/runner/run_single.py `
            --mode baseline `
            --n-agents 1 `
            --n-tasks $nTasks `
            --repeat-id $repeatId `
            --seed $seed `
            --units $units `
            --failure-injection-threshold 200 `
            --max-retries 1
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

        Write-Host "[RUN] ray n_tasks=$nTasks repeat=$repeatId"
        & $PythonBin src/runner/run_single.py `
            --mode ray `
            --n-agents 4 `
            --n-tasks $nTasks `
            --repeat-id $repeatId `
            --seed $seed `
            --units $units `
            --failure-injection-threshold 200 `
            --max-retries 1
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
}

Write-Host "[RUN] complete: $outCsv"
