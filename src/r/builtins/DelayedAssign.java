package r.builtins;

import r.data.*;
import r.errors.*;
import r.nodes.ast.*;
import r.nodes.exec.*;
import r.runtime.*;

final class DelayedAssign extends CallFactory {

    static final CallFactory _ = new DelayedAssign("delayedAssign", new String[]{"x", "value", "eval.env", "assign.env"}, new String[] {"x", "value"});

    private DelayedAssign(String name, String[] params, String[] required) {
        super(name, params, required);
    }

    @Override public RNode create(ASTNode call, RSymbol[] names, RNode[] exprs) {

        final ArgumentInfo ia = check(call, names, exprs);
        int evalPos = ia.position("eval.env");
        int assignPos = ia.position("assign.env");
        return new Assign(call, exprs[ia.position("x")], exprs[ia.position("value")], evalPos == -1 ? null : exprs[evalPos], assignPos == -1 ? null : exprs[assignPos]);
    }

    public static class Assign extends BaseR {
        @Child RNode xNode; // must be root node
        @Child RNode valueNode;
        @Child RNode evalEnvNode;
        @Child RNode assignEnvNode;

        public Assign(ASTNode ast, RNode xNode, RNode valueNode, RNode evalEnvNode, RNode assignEnvNode) {
            super(ast);
            this.xNode = adoptChild(xNode);
            this.valueNode = adoptChild(valueNode);
            this.evalEnvNode = adoptChild(evalEnvNode);
            this.assignEnvNode = adoptChild(assignEnvNode);
        }

        private REnvironment parseEnv(Frame frame, RNode envNode, String argName) {
            if (envNode == null) {
                return frame == null ? REnvironment.GLOBAL : frame.environment();
            } else {
                Object e = envNode.execute(frame);
                if (e instanceof REnvironment) {
                    return (REnvironment) e;
                }
                throw RError.getInvalidArgument(ast, argName);
            }
        }

        private Frame extractFrame(Frame frame, RNode envNode, String argName) {
            if (envNode == null) {
                return frame;
            } else {
                Object e = envNode.execute(frame);
                if (e instanceof REnvironment) {
                    return ((REnvironment) e).frame();
                }
                throw RError.getInvalidArgument(ast, argName);
            }
        }
        @Override
        public Object execute(Frame frame) {
            RSymbol xSymbol = EnvBase.parseXSilent(xNode.execute(frame), ast);
            Frame evalFrame = extractFrame(frame, evalEnvNode, "eval.env");
            REnvironment assignEnv = parseEnv(frame, assignEnvNode, "assign.env");
            assignEnv.delayedAssign(xSymbol, RPromise.createNormal(valueNode, evalFrame), ast);
            return RNull.getNull();
        }

        @Override
        protected <N extends RNode> N replaceChild(RNode oldNode, N newNode) {
            assert oldNode != null;
            if (xNode == oldNode) {
                xNode = newNode;
                return adoptInternal(newNode);
            }
            if (valueNode == oldNode) {
                valueNode = newNode;
                return adoptInternal(newNode);
            }
            if (evalEnvNode == oldNode) {
                evalEnvNode = newNode;
                return adoptInternal(newNode);
            }
            if (assignEnvNode == oldNode) {
                assignEnvNode = newNode;
                return adoptInternal(newNode);
            }
            return super.replaceChild(oldNode, newNode);
        }

    }

}
