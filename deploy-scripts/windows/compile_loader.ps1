$env:GOOS = "linux"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"

$filePath = Split-Path -parent $MyInvocation.MyCommand.Definition
$sourcePath = "$filePath\..\..\binaries"
$targetPath = "$filePath\..\..\target"

go build -o "$targetPath\loader" "$sourcePath\loader.go"

$env:GOOS = "windows"
$env:GOARCH = "amd64"

go build -o "$targetPath\loader.exe" "$sourcePath\loader.go"