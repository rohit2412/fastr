package r.fusion;

import r.data.*;
import r.data.internal.View;

import java.util.*;

/** Fusion manager for the views.
 *
 *
 */
public class Fusion {

    public static final boolean DEBUG = true;

    public static final boolean ENABLED = true;

    public static final boolean ENABLE_STATISTICS = true;

    public static final boolean VERIFY = true;

    /* The indices of these features are random numbers to give the hashing function broader scope and therefore less
     * chances of collisions. They are used throughout the fusion system to identify them.
     */
    static final int SCALAR =      -1753807778;
    static final int VECTOR =      -661390690;
    static final int DOUBLE =       1858103944;
    static final int INT =         -1860336380;
    static final int COMPLEX =      630400415;
    static final int ADD =          393835468;
    static final int SUB =          1744231241;
    static final int MUL =         -1486659162;
    static final int DIV =          2108082263;
    static final int MOD =         -604326540;
    static final int EQUAL =       -513021938;
    static final int A =            64259293;
    static final int B =            1000853063;
    static final int INPUT =        1825547246;
    static final int SEQUENCE =    -436454746;
    static final int BINARY =      -958270914;
    static final int CONVERSION =  -1803245152;
    static final int SUBSET =      -1649474957;
    static final int INT_SEQUENCE = INT + SEQUENCE;

    /** HashMap containing created fusion operators and their respective view signatures.
     */
    static final HashMap<Integer, FusedOperator.Prototype> operators = new HashMap<>();

    static int materialized = 0;

    static int hashFailed = 0;

    static int compiled = 0;

    static int compilationFailed = 0;

    static int reused = 0;


    /** Materializes the given view.
     *
     * Computes the hash of the view to determine if the view has already been compiled. If found, obtains the cached
     * fused operator, otherwise builds a new fused operator for the view.
     *
     * The fused operator is then executed to materialize the view.
     *
     * Note that if anything goes wrong a special fused operator that only calls the classic materialize in the view.
     *
     * @param view View to be materialized
     * @return Materialized contents of the view.
     */
    public static RArray materialize(View view) {
        if (ENABLE_STATISTICS)
            ++materialized;
        int hash = Hash.view(view);
        if (hash == 0) {
            if (ENABLE_STATISTICS)
                ++hashFailed;
            return view.materialize_();
        }
//        if (hash == 1727136857)
//            System.out.println("DEBUG");
        FusedOperator.Prototype fusedOperator = operators.get(hash);
        if (fusedOperator == null) {
            if (ENABLE_STATISTICS)
                ++compiled;
            fusedOperator = FusedOperator.build(view, hash);
            operators.put(hash, fusedOperator);
            if (ENABLE_STATISTICS && fusedOperator == FusedOperator.NO_FUSION)
                ++compilationFailed;
        } else if (ENABLE_STATISTICS) {
            ++reused;
        }
        RArray result = fusedOperator.materialize(view);
        if (VERIFY) {
            RArray check = view.materialize_();
//            if (result.getClass() != check.getClass())
//                throw new Error("FUSION: different class types");
            if (result.size() != check.size())
                throw new Error("FUSION: different result sizes");
            return check;
        }
        return result;
    }

    public static String statistics() {
        if (ENABLE_STATISTICS) {
            StringBuilder sb = new StringBuilder();
            sb.append("Total materialized:             "+materialized+"\n");
            sb.append("Hash failed (not supported):    "+hashFailed+"\n");
            sb.append("Compiled (including attempts):  "+compiled+"\n");
            sb.append("Compilation failed:             "+compilationFailed+"\n");
            sb.append("Reused:                         "+reused+"\n");
            sb.append("Cached:                         "+operators.size()+"\n");
            return sb.toString();
        } else {
            return "FUSION STATISTICS DISABLED - Enable by setting Fusion.ENABLE_STATISTICS to true.\n";
        }

    }

    public static void clearStatistics() {
        materialized = 0;
        hashFailed = 0;
        compiled = 0;
        compilationFailed = 0;
        reused = 0;
    }
}
