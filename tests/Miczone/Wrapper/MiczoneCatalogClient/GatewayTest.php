<?php

declare (strict_types = 1);

namespace Tests\Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Catalog\Breadcrumb\MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest;
use Miczone\Thrift\Catalog\Breadcrumb\MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequestItem;
use Miczone\Thrift\Catalog\Category\GetCategoryByIdRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryByOriginalCategoryRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryByProductSkuAndOriginalMerchantRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\Product\GetMatrixProductRequest;
use Miczone\Thrift\Catalog\Search\SearchProductRequest;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Wrapper\MiczoneCatalogClient\Gateway;
use PHPUnit\Framework\TestCase;

final class GatewayTest extends TestCase {

  /**
   * @var Gateway
   */
  private static $client;

  public static function setUpBeforeClass(): void {
    static::$client = new Gateway([
      'hosts' => '',
      'auth' => '',
      'sendTimeoutInMilliseconds' => 1000,
      'receiveTimeoutInMilliseconds' => 2000,
      'numberOfRetries' => 1,
      'scaleMode' => ScaleMode::BALANCING,
    ]);
  }

  public static function tearDownAfterClass(): void {
  }

  protected function setUp(): void {
  }

  protected function tearDown(): void {
  }

  public function testPing() {
    $result = static::$client->ping();

    $this->assertEquals(ErrorCode::SUCCESS, $result);
  }

  public function testSearchProduct() {
    $request = new SearchProductRequest();
    $request->categoryIdList = ['1'];

    $response = static::$client->searchProduct($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

  public function testGetCategoryById() {
    $request = new GetCategoryByIdRequest();
    $request->id = '12';
    $request->hasOriginalCategoryList = false;
    $request->hasBreadcrumbList = false;

    $response = static::$client->getCategoryById($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
    $this->assertEquals($request->id, $response->data->id);
  }

  public function testGetCategoryBySlug() {
    $request = new GetCategoryBySlugRequest();
    $request->slug = 'quan-ao-nu';
    $request->hasOriginalCategoryList = false;
    $request->hasBreadcrumbList = false;

    $response = static::$client->getCategoryBySlug($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
    $this->assertEquals($request->slug, $response->data->slug);
  }

  public function testGetCategoryByOriginalCategory() {
    $request = new GetCategoryByOriginalCategoryRequest();
    $request->websiteCode = 'amazon';
    $request->countryCode = 'us';
    $request->originalCategoryOriginalId = '1232597011';
    $request->hasBreadcrumbList = false;

    $response = static::$client->getCategoryByOriginalCategory($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

  public function testGetCategoryByProductSkuAndOriginalMerchant() {
    $request = new GetCategoryByProductSkuAndOriginalMerchantRequest();
    $request->websiteCode = 'amazon';
    $request->countryCode = 'us';
    $request->productSku = 'B00MPSJ0TW';
    $request->originalMerchantOriginalId = 'A34I76D6T5OLHY';
    $request->hasBreadcrumbList = false;

    $response = static::$client->getCategoryByProductSkuAndOriginalMerchant($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

  public function testMultiGetBreadcrumbListByProductSkuAndOriginalMerchant() {
    $requestDataMap = [];

    $requestDataMapItem1 = new MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequestItem();
    $requestDataMapItem1->websiteCode = 'amazon';
    $requestDataMapItem1->countryCode = 'us';
    $requestDataMapItem1->productSku = 'B00MPSJ0TW';
    $requestDataMapItem1->originalMerchantOriginalId = 'A34I76D6T5OLHY';

    $requestDataMap['item1'] = $requestDataMapItem1;

    $request = new MultiGetBreadcrumbListByProductSkuAndOriginalMerchantRequest();
    $request->dataMap = $requestDataMap;

    $response = static::$client->multiGetBreadcrumbListByProductSkuAndOriginalMerchant($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->dataMap);
  }

  public function testGetMatrixProduct() {
    $request = new GetMatrixProductRequest();
    $request->websiteCode = 'amazon';
    $request->countryCode = 'us';
    $request->productSku = 'B00ANOFPB2';
    $request->originalMerchantOriginalId = 'Test';

    $response = static::$client->getMatrixProduct($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

}
