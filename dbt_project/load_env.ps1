# Load .env file from root and set environment variables for the current PowerShell session
$envFile = Join-Path $PSScriptRoot "..\.env"

if (Test-Path $envFile) {
    Write-Host "Loading environment variables from $envFile..."
    foreach ($line in Get-Content $envFile) {
        # Skip empty lines and comments
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) {
            continue
        }

        # Split by the first '=' character
        $index = $line.IndexOf('=')
        if ($index -gt 0) {
            $name = $line.Substring(0, $index).Trim()
            $value = $line.Substring($index + 1).Trim()
            
            # Remove quotes if present
            if ($value.StartsWith('"') -and $value.EndsWith('"')) {
                $value = $value.Substring(1, $value.Length - 2)
            } elseif ($value.StartsWith("'") -and $value.EndsWith("'")) {
                $value = $value.Substring(1, $value.Length - 2)
            }

            # Local development overrides: when running dbt from host, connect to localhost:9080
            if ($name -eq "TRINO_HOST" -and $value -eq "trino") {
                $value = "localhost"
            }
            if ($name -eq "TRINO_PORT" -and $value -eq "8080") {
                $value = "9080"
            }

            [System.Environment]::SetEnvironmentVariable($name, $value, [System.EnvironmentVariableTarget]::Process)
            # Also set in the current session scope for immediate use
            Set-Item -Path "Env:\$name" -Value $value
        }
    }
    Write-Host "Environment variables loaded successfully."
} else {
    Write-Error "Error: .env file not found at $envFile"
}
