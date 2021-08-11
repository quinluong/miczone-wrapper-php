<?php

declare (strict_types = 1);

namespace Tests\Miczone\Wrapper;

use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\ScaleMode;
use Miczone\Thrift\Mail\ContentType;
use Miczone\Thrift\Mail\FromWebsite;
use Miczone\Wrapper\MiczoneMailClient;
use PHPUnit\Framework\TestCase;

final class MiczoneMailClientTest extends TestCase {

  /**
   * @var MiczoneMailClient
   */
  private static $client;

  public static function setUpBeforeClass(): void {
    static::$client = new MiczoneMailClient([
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

  public function testSend() {
    $this->markTestSkipped();

    $params = [
      'fromWebsite' => FromWebsite::FADO,
      'contentType' => ContentType::TEXT,
      'fromEmail' => 'support@fado.vn',
      'toList' => [
        [
          'email' => '',
          'name' => '',
        ],
      ],
      'subject' => 'Test subject',
      'content' => 'Test content',
    ];

    $response = static::$client->send($params);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
  }

  public function testOwSend() {
    $this->markTestSkipped();

    $params = [
      'fromWebsite' => FromWebsite::FADO,
      'contentType' => ContentType::TEXT,
      'fromEmail' => 'support@fado.vn',
      'toList' => [
        [
          'email' => '',
          'name' => '',
        ],
      ],
      'subject' => 'Test subject oneway',
      'content' => 'Test content oneway',
    ];

    static::$client->ow_send(
      $params,
      function () {
        $this->assertTrue(true);
      },
      function () {
        $this->assertTrue(false);
      }
    );
  }

  public function testGetInfo() {
    $params = [
      'fromWebsite' => FromWebsite::FADO,
      'partition' => 17,
      'offset' => 68531,
    ];

    $response = static::$client->getInfo($params);

    var_dump($response);

    $this->assertNotNull($response);
    $this->assertNotNull($response->error);
    $this->assertEquals(ErrorCode::SUCCESS, $response->error->code);
    $this->assertNotNull($response->data);
  }

}
