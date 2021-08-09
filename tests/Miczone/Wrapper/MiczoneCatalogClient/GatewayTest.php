<?php

declare (strict_types = 1);

namespace Tests\Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Wrapper\MiczoneCatalogClient\Gateway;
use PHPUnit\Framework\TestCase;

final class GatewayTest extends TestCase {

  private static $client;

  public static function setUpBeforeClass(): void {
    static::$client = new Gateway([
      'hosts' => '',
      'auth' => '',
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

}
