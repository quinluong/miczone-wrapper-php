<?php

declare (strict_types = 1);

namespace Tests\Miczone\Wrapper\MiczoneEventBusClient;

use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Wrapper\MiczoneEventBusClient\Notifier;
use PHPUnit\Framework\TestCase;

final class NotifierTest extends TestCase {

  private static $topic = 'miczone.eventbus.topic.test';
  private static $key = 'test-key';

  /**
   * @var Notifier
   */
  private static $client;

  public static function setUpBeforeClass(): void {
    static::$client = new Notifier([
      'hosts' => '',
      'clientId' => 'miczone.eventbus.client.test',
      'auth' => '',
      'topics' => static::$topic,
      'sendTimeoutInMilliseconds' => 1000,
      'receiveTimeoutInMilliseconds' => 1000,
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

  public function testNotifyBoolean() {
    $params = [
      'topic' => static::$topic,
      'data' => true,
      'key' => static::$key,
    ];

    $response = static::$client->notifyBoolean($params);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwNotifyBoolean() {
    $params = [
      'topic' => static::$topic,
      'data' => true,
      'key' => static::$key,
    ];

    static::$client->ow_notifyBoolean(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testNotifyInteger() {
    $params = [
      'topic' => static::$topic,
      'data' => 123,
      'key' => static::$key,
    ];

    $response = static::$client->notifyInteger($params);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwNotifyInteger() {
    $params = [
      'topic' => static::$topic,
      'data' => 123,
      'key' => static::$key,
    ];

    static::$client->ow_notifyInteger(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testNotifyDouble() {
    $params = [
      'topic' => static::$topic,
      'data' => 123.456,
      'key' => static::$key,
    ];

    $response = static::$client->notifyDouble($params);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwNotifyDouble() {
    $params = [
      'topic' => static::$topic,
      'data' => 123.456,
      'key' => static::$key,
    ];

    static::$client->ow_notifyDouble(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testNotifyString() {
    $params = [
      'topic' => static::$topic,
      'data' => 'Hello world !',
      'key' => static::$key,
    ];

    $response = static::$client->notifyString($params);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwNotifyString() {
    $params = [
      'topic' => static::$topic,
      'data' => 'Hello world !',
      'key' => static::$key,
    ];

    static::$client->ow_notifyString(
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
