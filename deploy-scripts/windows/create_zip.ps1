$env:GOOS = "linux"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"

$filePath = Split-Path -parent $MyInvocation.MyCommand.Definition
$handlersBasePath = "$filePath\..\..\handlers"

$handlersList = Get-ChildItem -path "$handlersBasePath"

foreach ($handlerName in $handlersList) {
    $handlerPath = "$handlersBasePath\$handlerName"
    go build -o "$handlerPath\bootstrap" "$handlerPath\$handlerName.go"
    build-lambda-zip -o "$handlerPath\$handlerName.zip" "$handlerPath\bootstrap"
    rm "$handlerPath\bootstrap"
}
