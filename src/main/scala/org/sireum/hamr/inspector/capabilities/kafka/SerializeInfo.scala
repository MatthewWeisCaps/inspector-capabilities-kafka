package org.sireum.hamr.inspector.capabilities.kafka

import art.Art.{PortId, Time}
import art.DataContent

case class SerializeInfo(src: PortId, dst: PortId, data: DataContent)//, time: Time)