azurite-pull:
	npm install -g azurite

azurite-up:
	azurite --silent --location c:\azurite --debug c:\azurite\debug.log

pack:
	dotnet pack --configuration Release --include-symbols --include-source -p:SymbolPackageFormat=snupkg -p:PackageVersion=1.0.0 -o out/ AzureBatchQueue/

start-storage:
	docker run -p 10000:10000 -p 10001:10001 mcr.microsoft.com/azure-storage/azurite
