FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ABDLab3.Producer.csproj ./
RUN dotnet restore

COPY . ./
RUN dotnet publish -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app ./

ENTRYPOINT ["dotnet", "ABDLab3.Producer.dll"]
