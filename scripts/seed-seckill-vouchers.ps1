param(
    [string]$BaseUrl = 'http://127.0.0.1:8081',
    [ValidateRange(1, 10000)] [int]$Stock = 30
)

$ErrorActionPreference = 'Stop'
$market = Invoke-RestMethod -Uri "$BaseUrl/voucher/market" -TimeoutSec 10
if (-not $market.success) { throw 'Cannot load the LocalPerks voucher market.' }

$begin = (Get-Date).AddMinutes(-1).ToString('yyyy-MM-ddTHH:mm:ss')
$end = (Get-Date).AddDays(30).ToString('yyyy-MM-ddTHH:mm:ss')
$shopsWithSeckill = @($market.data | Where-Object type -eq 1 | Select-Object -ExpandProperty shopId)

# These local demo prices are in cents. Only seed merchants with existing regular offers.
$demoOffers = @(
    @{ ShopId = 4; Pay = 1900; Value = 3000 },
    @{ ShopId = 5; Pay = 1700; Value = 2500 },
    @{ ShopId = 6; Pay = 6900; Value = 10000 },
    @{ ShopId = 13; Pay = 10900; Value = 15000 },
    @{ ShopId = 20; Pay = 8500; Value = 12800 },
    @{ ShopId = 26; Pay = 3900; Value = 6000 }
)

foreach ($offer in $demoOffers) {
    $regular = $market.data | Where-Object { $_.shopId -eq $offer.ShopId -and $_.type -eq 0 } | Select-Object -First 1
    if (-not $regular -or $offer.ShopId -in $shopsWithSeckill) { continue }

    $payload = @{
        shopId = $offer.ShopId
        title = '限时秒杀 · ' + $regular.shopName
        subTitle = '限量代金券，到店使用'
        rules = '到店使用；使用规则以店铺说明为准。'
        payValue = $offer.Pay
        actualValue = $offer.Value
        status = 1
        stock = $Stock
        beginTime = $begin
        endTime = $end
    } | ConvertTo-Json -Compress
    $result = Invoke-RestMethod -Uri "$BaseUrl/voucher/seckill" -Method Post -TimeoutSec 10 `
        -ContentType 'application/json; charset=utf-8' -Body ([Text.Encoding]::UTF8.GetBytes($payload))
    if (-not $result.success) { throw "Could not seed shop $($offer.ShopId): $($result.errorMsg)" }
    [PSCustomObject]@{ ShopId = $offer.ShopId; VoucherId = $result.data; Stock = $Stock }
}
