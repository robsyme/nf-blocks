package nextflow.blockstore

import io.ipfs.cid.Cid
import io.ipfs.multibase.Multibase
import io.ipfs.multihash.Multihash
import spock.lang.Specification

class CidLinkTest extends Specification {
    
    def "should create CidLink from Cid object"() {
        given:
        Cid cid = Cid.decode("zdpuAyvkgEDQm9TenwGkd5eNaosSxjgEYd8QatfPetgB1CdEZ");
        
        when:
        def link = new CidLink(cid)
        
        then:
        noExceptionThrown()
        link.cid == cid
    }
    
    def "should throw exception for null Cid"() {
        when:
        new CidLink(null)
        
        then:
        thrown(NullPointerException)
    }
        
    def "should encode to DAG-CBOR"() {
        given:
        def cid = Cid.decode("QmQg1v4o9xdT3Q14wh4S7dxZkDjyZ9ssFzFzyep1YrVJBY")
        def link = new CidLink(cid)
        
        when:
        def encoded = link.toCbor().toByteArray()
        
        then:
        encoded.encodeBase64().toString() == "2CpYIwASICKtYxxp7pgwlbW4rNAp/5Sv8dxsSIN4eFiakrkN/qMX"
    }
        
    def "should convert to string representation"() {
        given:
        Cid cid = Cid.decode("zdpuAyvkgEDQm9TenwGkd5eNaosSxjgEYd8QatfPetgB1CdEZ");
        def link = new CidLink(cid)
        
        expect:
        link.toString() == "CidLink(${cid.toString()})"
    }
    
    def "should throw exception for empty CID bytes"() {
        when:
        CidLink.fromCid([] as byte[])
        
        then:
        thrown(IllegalArgumentException)
    }
} 