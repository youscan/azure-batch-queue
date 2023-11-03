azurite-pull:
	npm install -g azurite

azurite-up:
	azurite --silent --location c:\azurite --debug c:\azurite\debug.log

pack:
	dotnet pack --configuration Release --include-symbols --include-source -p:SymbolPackageFormat=snupkg -p:PackageVersion=1.0.0 -o out/ AzureBatchQueue/
