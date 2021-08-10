<?php

declare (strict_types = 1);

namespace Tests\Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Catalog\Category\GetCategoryByIdRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\Category\GetCategoryMappingByIdRequest;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryByIdRequest;
use Miczone\Thrift\Catalog\Category\MultiGetCategoryBySlugRequest;
use Miczone\Thrift\Catalog\Product\GetProductByIdRequest;
use Miczone\Thrift\Catalog\Product\GetSliceProductRequest;
use Miczone\Thrift\Catalog\Product\MultiGetProductByIdRequest;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Wrapper\MiczoneCatalogClient\Storage;
use PHPUnit\Framework\TestCase;

final class StorageTest extends TestCase {

  /**
   * @var Storage
   */
  private static $client;

  public static function setUpBeforeClass(): void {
    static::$client = new Storage([
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

  public function testGetProductById() {
    $request = new GetProductByIdRequest();
    $request->id = '726410473502806016';

    $response = static::$client->getProductById($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
    $this->assertEquals($request->id, $response->data->id);
  }

  public function testMultiGetProductByIdList() {
    $this->markTestSkipped();

    $request = new MultiGetProductByIdRequest();
    $request->idList = ['726410473502806016', '726410473502806017'];

    $response = static::$client->multiGetProductByIdList($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->dataMap);
  }

  public function testGetSliceProduct() {
    $request = new GetSliceProductRequest();
    $request->fromId = '726410473502806016';
    $request->limit = 3;

    $response = static::$client->getSliceProduct($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
    $this->assertEquals($request->limit, count($response->data->itemList));
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

  public function testMultiGetCategoryByIdList() {
    $this->markTestSkipped();

    $request = new MultiGetCategoryByIdRequest();
    $request->idList = ['12'];
    $request->hasOriginalCategoryList = false;
    $request->hasBreadcrumbList = false;

    $response = static::$client->multiGetCategoryByIdList($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->dataMap);
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

  public function testMultiGetCategoryBySlugList() {
    $this->markTestSkipped();

    $request = new MultiGetCategoryBySlugRequest();
    $request->slugList = ['quan-ao-nu'];
    $request->hasOriginalCategoryList = false;
    $request->hasBreadcrumbList = false;

    $response = static::$client->multiGetCategoryBySlugList($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->dataMap);
  }

  public function testGetCategoryMappingById() {
    $request = new GetCategoryMappingByIdRequest();
    $request->id = '1';
    $request->childDepth = 1;

    $response = static::$client->getCategoryMappingById($request);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

}
