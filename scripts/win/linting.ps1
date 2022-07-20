Write-Output "Creating the Scalastyle-config.xml with  default setting checks "
sbt scalastyleGenerateConfig

Write-Output "Running scala check"
sbt scalastyle
