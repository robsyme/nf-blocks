package robsyme.cas.core

import java.nio.file.Path
import spock.lang.TempDir

class LocalCoordinateTreeContractTest extends CoordinateTreeContract {
    @TempDir Path dir
    @Override CoordinateTree tree() { new LocalCoordinateTree(dir.resolve('coords')) }
}
