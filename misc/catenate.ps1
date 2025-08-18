# Set the source and output paths
$sourceFolder = "docs/design"
$outputFile = "docs/design/all-designs-catenated.md"

# Get all markdown files in the source folder, sorted by name
$files = Get-ChildItem -Path $sourceFolder -Filter *.md | Sort-Object Name

# Create or clear the output file
Set-Content -Path $outputFile -Value ""

foreach ($file in $files) {
    # Write a breaking point and filename as heading
    Add-Content -Path $outputFile -Value "`n---`n# $($file.Name)`n---`n"
    # Append the file content
    Get-Content -Path $file.FullName | Add-Content -Path $outputFile
}

Write-Host "Concatenation complete. Output file: $outputFile"