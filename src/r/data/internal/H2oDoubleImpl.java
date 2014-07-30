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

public class H2oDoubleImpl extends NonScalarArrayImpl implements RDouble, View.H2oView {

    public static final int MinSize = 20000;
    public static final int CHKSize = 100;
    //final double[] content;
    public final Frame frame;

    public static final boolean isH2o(RDouble a){
        return (a instanceof ProfilingView && ((View.RDoubleProxy)a).orig instanceof View.H2oView) || a instanceof View.H2oView;
    }

    private Frame create(double[] values, int[] dimensions) {
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
    public double[] getContent() {
        double[] content  = new double[(int) (frame.numRows()*frame.numCols())];
        for (int row = 0; row<frame.numRows(); row++){
            for (int col = 0; col<frame.numCols(); col++){
                content[row*frame.numCols()+col] = frame.vec(col).at(row);
            }
        }
        return content;
    }

    public H2oDoubleImpl(Frame frame){
        System.out.println("Wrapping a H2O Object in R");
        this.frame = frame;
        this.dimensions = new int[]{(int)frame.numRows(), frame.numCols()};
    }

    public H2oDoubleImpl(double[] values, int[] dimensions, Names names, Attributes attributes) {
        frame = create(values, dimensions);
        this.dimensions = dimensions;
        this.names = names;
        this.attributes = attributes;
    }

    public H2oDoubleImpl(double[] values, int[] dimensions, Names names) {
        this(values, dimensions, names, null);
    }

    public H2oDoubleImpl(RDouble d) {
        frame = create(d.getContent(), d.dimensions());
        dimensions = d.dimensions();
        names = d.names();
        attributes = d.attributes();
    }

    public H2oDoubleImpl(RDouble d, int[] dimensions, Names names, Attributes attributes) {
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
        return frame.vec(col).at(row);
    }

    @Override
    public RAny boxedGet(int i) {
        return RDoubleFactory.getScalar(getDouble(i));
    }

    @Override
    public boolean isNAorNaN(int i) {
        return RDoubleUtils.isNAorNaN(getDouble(i));
    }

    @Override
    public RArray set(int i, Object val) {
        return set(i, ((Double) val).doubleValue()); // FIXME better conversion
    }

    @Override
    public RDouble set(int i, double val) {
        int row = i / frame.numCols();
        int col = i % frame.numCols();
        frame.vec(col).set(row, val);
        frame.vec(col).postWrite();
        return this;
    }

    @Override
    public double getDouble(int i) {
        return ((Double) get(i)).doubleValue();
    }

    @Override
    public H2oDoubleImpl materialize() {
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
            str.append(Convert.prettyNA(Convert.double2string(getDouble(0))));
            int size=size();
            for (int i = 1; i < size; i++) {
                str.append(", ");
                str.append(Convert.prettyNA(Convert.double2string(getDouble(i))));
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
        return RDoubleUtils.doubleToRaw(this, warn);
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
        return TracingView.ViewTrace.trace(new RIntView(this));
    }

    @Override
    public RInt asInt(ConversionStatus warn) {
        return RDoubleUtils.double2int(this, warn);
    }

    @Override
    public RDouble asDouble() {
        return this;
    }

    @Override
    public RDouble asDouble(ConversionStatus warn) {
        return this;
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
        return RDoubleFactory.subset(this, index);
    }

    @Override
    public String typeOf() {
        return RDouble.TYPE_STRING;
    }

    @Override
    public H2oDoubleImpl doStrip() {
        return this;
    }

    @Override
    public H2oDoubleImpl doStripKeepNames() {
        return this;
    }

    @Override
    public double sum(boolean narm) {
        double res = 0;
        int size = size();
        for (int i = 0; i < size; i++) {
            double d = getDouble(i);
            if (narm) {
                if (RDoubleUtils.isNAorNaN(d)) {
                    continue;
                }
            }
            res += d;
        }
        return res;
    }

    @Override
    public void accept(ValueVisitor v) {
        v.visit(this);
    }
}
