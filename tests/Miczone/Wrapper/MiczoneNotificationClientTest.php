<?php

declare (strict_types = 1);

namespace Tests\Miczone\Wrapper;

use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Thrift\Notification\NotificationItemTargetUserType;
use Miczone\Wrapper\MiczoneNotificationClient;
use PHPUnit\Framework\TestCase;

final class MiczoneNotificationClientTest extends TestCase {

  private static $publisher = 'fado';
  private static $userId = 163449;

  /**
   * @var MiczoneNotificationClient
   */
  private static $client;

  public static function setUpBeforeClass(): void {
    static::$client = new MiczoneNotificationClient([
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

  public function testGetCounter() {
    $params = [
      'publisher' => static::$publisher,
      'userId' => static::$userId,
    ];

    $response = static::$client->getCounter($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

  public function testResetCounter() {
    $params = [
      'publisher' => static::$publisher,
      'userId' => static::$userId,
    ];

    $response = static::$client->resetCounter($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);

    $response = static::$client->getCounter($params);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
    $this->assertEquals(0, $response->data);
  }

  public function testOwResetCounter() {
    $params = [
      'publisher' => static::$publisher,
      'userId' => static::$userId,
    ];

    static::$client->ow_resetCounter(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testGetItemList() {
    $params = [
      'publisher' => static::$publisher,
      'userId' => static::$userId,
    ];

    $response = static::$client->getItemList($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

  public function testInsertItem() {
    $params = [
      'publisher' => static::$publisher,
      'targetUserType' => NotificationItemTargetUserType::REGISTERED,
      'registeredUserIdSet' => [
        static::$userId,
      ],
      'id' => 1,
      'groupId' => 1,
      'typeId' => 20,
      'title' => 'Test notification',
    ];

    $response = static::$client->insertItem($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwInsertItem() {
    $params = [
      'publisher' => static::$publisher,
      'targetUserType' => NotificationItemTargetUserType::REGISTERED,
      'registeredUserIdSet' => [
        static::$userId,
      ],
      'id' => 2,
      'groupId' => 1,
      'typeId' => 20,
      'title' => 'Test notification',
    ];

    static::$client->ow_insertItem(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testUpdateItem() {
    $params = [
      'publisher' => static::$publisher,
      'id' => 1,
      'title' => 'Test notification (updated)',
    ];

    $response = static::$client->updateItem($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwUpdateItem() {
    $params = [
      'publisher' => static::$publisher,
      'id' => 2,
      'title' => 'Test notification (updated)',
    ];

    static::$client->ow_updateItem(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testUpdateReadItem() {
    $params = [
      'publisher' => static::$publisher,
      'userId' => static::$userId,
      'id' => 465138,
      'isRead' => true,
    ];

    $response = static::$client->updateReadItem($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwUpdateReadItem() {
    $params = [
      'publisher' => static::$publisher,
      'userId' => static::$userId,
      'id' => 465138,
      'isRead' => true,
    ];

    static::$client->ow_updateReadItem(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testDeleteItem() {
    $params = [
      'publisher' => static::$publisher,
      'id' => 3,
    ];

    $response = static::$client->deleteItem($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwDeleteItem() {
    $params = [
      'publisher' => static::$publisher,
      'id' => 4,
    ];

    static::$client->ow_deleteItem(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

}
