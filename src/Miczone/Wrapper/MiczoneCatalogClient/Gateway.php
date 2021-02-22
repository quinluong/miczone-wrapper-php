<?php

namespace Miczone\Wrapper\MiczoneCatalogClient;

use Miczone\Thrift\Catalog\MiczoneCatalogGatewayServiceClient;
use Miczone\Thrift\Catalog\SearchProductRequest;
use Miczone\Thrift\Catalog\SearchProductResponse;
use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class Gateway {
  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const AUTH = '';

  const SEND_TIMEOUT_IN_MILLISECONDS = 5000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 5000;

  const NUMBER_OF_RETRIES = 3;

  protected $config;

  protected $operationHandle;

  protected $lastException;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'auth' => static::AUTH,
      'sendTimeoutInMilliseconds' => static::SEND_TIMEOUT_IN_MILLISECONDS,
      'receiveTimeoutInMilliseconds' => static::RECEIVE_TIMEOUT_IN_MILLISECONDS,
      'numberOfRetries' => static::NUMBER_OF_RETRIES,
    ], $config);

    $config['hosts'] = $this->_standardizeHosts($config['hosts']);

    if (empty($config['hosts'])) {
      throw new \Exception('Invalid "hosts" config');
    }

    $config['auth'] = $this->_standardizeAuth($config['auth']);

    if (empty($config['auth'])) {
      throw new \Exception('Invalid "auth" config');
    }

    if (!is_int($config['sendTimeoutInMilliseconds']) || $config['sendTimeoutInMilliseconds'] <= 0) {
      $config['sendTimeoutInMilliseconds'] = static::SEND_TIMEOUT_IN_MILLISECONDS;
    }

    if (!is_int($config['receiveTimeoutInMilliseconds']) || $config['receiveTimeoutInMilliseconds'] <= 0) {
      $config['receiveTimeoutInMilliseconds'] = static::RECEIVE_TIMEOUT_IN_MILLISECONDS;
    }

    if (!is_int($config['numberOfRetries']) || $config['numberOfRetries'] <= 0) {
      $config['numberOfRetries'] = static::NUMBER_OF_RETRIES;
    }

    $this->config = $config;

    $this->operationHandle = new OperationHandle([
      'username' => $this->config['auth']['username'],
      'password' => $this->config['auth']['password'],
    ]);
  }

  public function __destruct() {
  }

  private function _standardizeHosts(string $hosts) {
    if (empty($hosts)) {
      return [];
    }

    $hosts = explode(',', $hosts);

    if (empty($hosts)) {
      return [];
    }

    $result = [];

    foreach ($hosts as $item) {
      $hostPortPair = explode(':', $item);
      if (count($hostPortPair) !== 2) {
        continue;
      }
      $host = trim($hostPortPair[0]);
      $port = (int) trim($hostPortPair[1]);
      if (empty($host) || $port <= 0) {
        continue;
      }
      \array_push($result, [
        'host' => $host,
        'port' => $port,
      ]);
    }

    return $result;
  }

  private function _standardizeAuth(string $auth) {
    if (empty($auth)) {
      return [];
    }

    $auth = explode(':', $auth);

    if (count($auth) !== 2) {
      return [];
    }

    $username = trim($auth[0]);
    $password = trim($auth[1]);

    if (empty($username) || empty($password)) {
      return [];
    }

    $result = [
      'username' => $username,
      'password' => $password,
    ];

    return $result;
  }

  private function _createSearchProductRequest(array $params = []) {
    $request = new SearchProductRequest();

    if (isset($params['categoryId']) && is_string($params['categoryId']) && trim($params['categoryId']) !== '') {
      $request->categoryId = trim($params['categoryId']);
    }

    if (isset($params['website']) && is_string($params['website']) && trim($params['website']) !== '') {
      $request->website = trim($params['website']);
    }

    if (isset($params['countryCode']) && is_string($params['countryCode']) && trim($params['countryCode']) !== '') {
      $request->countryCode = trim($params['countryCode']);
    }

    if (isset($params['name']) && is_string($params['name']) && trim($params['name']) !== '') {
      $request->name = trim($params['name']);
    }

    if (isset($params['translatedName']) && is_string($params['translatedName']) && trim($params['translatedName']) !== '') {
      $request->translatedName = trim($params['translatedName']);
    }

    if (isset($params['ratingStar']) && is_float($params['ratingStar']) && $params['ratingStar'] >= 0) {
      $request->ratingStar = $params['ratingStar'];
    }

    if (isset($params['merchantId']) && is_string($params['merchantId']) && trim($params['merchantId']) !== '') {
      $request->merchantId = trim($params['merchantId']);
    }

    if (isset($params['merchantName']) && is_string($params['merchantName']) && trim($params['merchantName']) !== '') {
      $request->merchantName = trim($params['merchantName']);
    }

    if (isset($params['brandId']) && is_string($params['brandId']) && trim($params['brandId']) !== '') {
      $request->brandId = trim($params['brandId']);
    }

    if (isset($params['brandName']) && is_string($params['brandName']) && trim($params['brandName']) !== '') {
      $request->brandName = trim($params['brandName']);
    }

    if (isset($params['minPrice']) && is_float($params['minPrice']) && $params['minPrice'] >= 0) {
      $request->minPrice = $params['minPrice'];
    }

    if (isset($params['maxPrice']) && is_float($params['maxPrice']) && $params['maxPrice'] >= 0) {
      $request->maxPrice = $params['maxPrice'];
    }

    if (isset($params['sortBy']) && is_string($params['sortBy']) && trim($params['sortBy']) !== '') {
      $request->sortBy = trim($params['sortBy']);
    }

    if (isset($params['productCount']) && is_int($params['productCount']) && $params['productCount'] > 0) {
      $request->productCount = $params['productCount'];
    }

    if (isset($params['productPage']) && is_int($params['productPage']) && $params['productPage'] > 0) {
      $request->productPage = $params['productPage'];
    }

    return $request;
  }

  private function _createTransportAndClient(string $host, int $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->config['receiveTimeoutInMilliseconds']);
    $socket->setRecvTimeout($this->config['sendTimeoutInMilliseconds']);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneCatalogGatewayServiceClient($protocol);

    return [$transport, $client];
  }

  public function getLastException() {
    return $this->lastException;
  }

  /**
   * @return \Miczone\Thrift\Common\ErrorCode
   */
  public function ping() {
    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->ping($this->operationHandle);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return ErrorCode::THRIFT_BAD_REQUEST;
  }

  /**
   * @param array $params Contains:
   * string     categoryId
   * string     website
   * string     countryCode
   * string     name
   * string     translatedName
   * float      ratingStar
   * string     merchantId
   * string     merchantName
   * string     brandId
   * string     brandName
   * float      minPrice
   * float      maxPrice
   * string     sortBy
   * int        productCount
   * int        productPage
   * @return \Miczone\Thrift\Catalog\SearchProductResponse
   * @throws \Exception
   */
  public function searchProduct(array $params) {
    $request = $this->_createSearchProductRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->searchProduct($this->operationHandle, $request);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return new SearchProductResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

}
