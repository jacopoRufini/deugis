package org.k2.dataIngestion.models

import org.locationtech.jts.geom.Envelope

case class RelationMetadata(geometryClass: String, wkt: String, centroid: String, envelopeString: String, envelopeObject: Envelope)