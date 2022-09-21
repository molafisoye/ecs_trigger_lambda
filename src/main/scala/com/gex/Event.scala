package com.gex

sealed trait Event

class APIGatewayV2HTTPEvent extends Event
class S3Event extends Event
