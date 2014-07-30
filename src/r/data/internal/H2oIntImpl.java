package r.data.internal;

import r.Convert;
import r.Convert.ConversionStatus;
import r.data.*;
import water.Futures;
import water.Key;
import water.fvec.AppendableVec;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;

public class H2oIntImpl extends NonScalarArrayImpl implements RInt, View.H2oView {

    public static final int MinSize = 20000;
    public static final int CHKSize = 100;
    //final double[] content;
    public final Frame frame;

    public static final boolean isH2o(RInt a){
        return (a instanceof ProfilingView && ((View.RIntProxy)a).orig instanceof View.H2oView) || a instanceof View.H2oView;
    }


    private Frame create(int[] values, int[] dimensions) {
        System.out.println("Storing R Object in H2O");
        Futures fs = new Futures();
        Vec[] vecs = new Vec[dimensions[1]];
        Key keys[] = new Vec.VectorGroup().addVecs(vecs.length);

        for( int c = 0; c < vecs.length; c++ ) {
            int chkSize = CHKSize;
            AppendableVec vec = new AppendableVec(keys[c]);
            int cidx=0;
            NewChunk chunk = new NewChunk(vec,0);
            for( int r = 0; r < dimensions[0]; r++ ) {
                chunk.addNum(values[r*dimensions[1]+c]);
                if ((r+1)%chkSize ==0 ) {
                    chunk.close(cidx, fs);
                    cidx++;
                    chunk = new NewChunk(vec,cidx);}
            }
            chunk.close(cidx, fs);
            vecs[c] = vec.close(fs);
        }
        fs.blockForPending();
        return new Frame(null, null, vecs);
    }

    @Override
    public int[] getContent() {
        int[] content  = new int[(int) (frame.numRows()*frame.numCols())];
        for (int row = 0; row<frame.numRows(); row++){
            for (int col = 0; col<frame.numCols(); col++){
                content[row*frame.numCols()+col] = (int)frame.vec(col).at(row);
            }
        }
        return content;
    }

    public H2oIntImpl(Frame frame){
        System.out.println("Wrapping a H2O Object in R");
        this.frame = frame;
        this.dimensions = new int[]{(int)frame.numRows(), frame.numCols()};
    }

    public H2oIntImpl(int[] values, int[] dimensions, Names names, Attributes attributes) {
        frame = create(values, dimensions);
        this.dimensions = dimensions;
        this.names = names;
        this.attributes = attributes;
    }

    public H2oIntImpl(int[] values, int[] dimensions, Names names) {
        this(values, dimensions, names, null);
    }

    public H2oIntImpl(RInt d) {
        frame = create(d.getContent(), d.dimensions());
        dimensions = d.dimensions();
        names = d.names();
        attributes = d.attributes();
    }

    public H2oIntImpl(RInt d, int[] dimensions, Names names, Attributes attributes) {
        frame = create(d.getContent(), dimensions);
        this.dimensions = dimensions;
        this.names = names;
        this.attributes = attributes;
    }

    @Override
    public int size() {
        return (int) (frame.numCols()*frame.numRows());
    }

    @Override
    public Object get(int i) {
        int row = i / frame.numCols();
        int col = i % frame.numCols();
        return (int) (long) frame.vec(col).at8(row);
    }

    @Override
    public RAny boxedGet(int i) {
        return RIntFactory.getScalar(getInt(i));
    }

    @Override
    public boolean isNAorNaN(int i) {
        return getInt(i) == RInt.NA;
    }

    @Override
    public RArray set(int i, Object val) {
        return set(i, ((Integer) val).intValue()); // FIXME better conversion
    }

    @Override
    public RInt set(int i, int val) {
        int row = i / frame.numCols();
        int col = i % frame.numCols();
        frame.vec(col).set(row, val);
        frame.vec(col).postWrite();
        return this;
    }

    @Override
    public int getInt(int i) {
        return ((Integer) get(i)).intValue();
    }

    @Override
    public H2oIntImpl materialize() {
        return this;
    }

    private static final String EMPTY_STRING = "numeric(0)"; // NOTE: this is not RDouble.TYPE_STRING (R is inconsistent on this)
    private static final String NAMED_EMPTY_STRING = "named " + EMPTY_STRING;

    @Override
    public String pretty() {
        StringBuilder str = new StringBuilder();
        if (dimensions != null) {
            str.append(arrayPretty());
        } else if (frame.numRows() == 0) {
            str.append((names() == null) ? EMPTY_STRING : NAMED_EMPTY_STRING);
        } else if (names() != null) {
            str.append(namedPretty());
        } else {
            str.append(Convert.prettyNA(Convert.double2string(getInt(0))));
            int size = size();
            for (int i = 1; i < size; i++) {
                str.append(", ");
                str.append(Convert.prettyNA(Convert.double2string(getInt(i))));
            }
        }
        str.append(attributesPretty());
        return str.toString();
    }

    @Override
    public RRaw asRaw() {
        return TracingView.ViewTrace.trace(new RRawView(this));
    }

    @Override
    public RRaw asRaw(ConversionStatus warn) {
        return RIntUtils.intToRaw(this, warn);
    }

    @Override
    public RLogical asLogical() {
        return TracingView.ViewTrace.trace(new RLogicalView(this));
    }

    @Override
    public RLogical asLogical(ConversionStatus warn) {
        return asLogical();
    }

    @Override
    public RInt asInt() {
        return this;
    }

    @Override
    public RInt asInt(ConversionStatus warn) {
        return this;
    }

    @Override
    public RDouble asDouble() {
        return new H2oDoubleImpl(frame);
    }



    @Override
    public RDouble asDouble(ConversionStatus warn) {
        return asDouble();
    }

    @Override
    public RComplex asComplex() {
        return TracingView.ViewTrace.trace(new RComplexView(this));
    }

    @Override
    public RComplex asComplex(ConversionStatus warn) {
        return asComplex();
    }

    @Override
    public RString asString() {
        return TracingView.ViewTrace.trace(new RStringView(this));
    }

    @Override
    public RString asString(ConversionStatus warn) {
        return asString();
    }

    @Override
    public RArray subset(RInt index) {
        return RIntFactory.subset(this, index);
    }

    @Override
    public String typeOf() {
        return RInt.TYPE_STRING;
    }

    @Override
    public H2oIntImpl doStrip() {
        return this;
    }

    @Override
    public H2oIntImpl doStripKeepNames() {
        return this;
    }

    @Override
    public void accept(ValueVisitor v) {
        v.visit(this);
    }
}
