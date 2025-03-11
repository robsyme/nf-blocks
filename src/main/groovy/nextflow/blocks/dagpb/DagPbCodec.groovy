package nextflow.blocks.dagpb

import groovy.transform.CompileStatic
import com.google.protobuf.ByteString
import nextflow.blocks.dagpb.proto.DagPb.PBNode as ProtoPBNode
import nextflow.blocks.dagpb.proto.DagPb.PBLink as ProtoPBLink

@CompileStatic
class DagPbCodec {
    static byte[] encode(DagPbNode node) {
        def builder = ProtoPBNode.newBuilder()
        
        if (node.data) {
            builder.setData(ByteString.copyFrom(node.data))
        }
        
        node.links.each { link ->
            def linkBuilder = ProtoPBLink.newBuilder()
            linkBuilder.setHash(ByteString.copyFrom(link.hash))
            if (link.name) {
                linkBuilder.setName(link.name)
            }
            if (link.tsize != null) {
                linkBuilder.setTsize(link.tsize)
            }
            builder.addLinks(linkBuilder)
        }
        
        return builder.build().toByteArray()
    }
    
    static DagPbNode decode(byte[] bytes) {
        def protoNode = ProtoPBNode.parseFrom(bytes)
        
        def links = protoNode.linksList.collect { link ->
            new DagPbLink(
                link.hash.toByteArray(),
                link.hasName() ? link.name : null,
                link.hasTsize() ? link.tsize : null
            )
        }
        
        return new DagPbNode(
            links,
            protoNode.hasData() ? protoNode.data.toByteArray() : null
        )
    }
} 