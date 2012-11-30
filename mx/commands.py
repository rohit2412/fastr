
import os, sys, shutil;
from os.path import exists, join;
import mx;

# Graal VM module
gmod = None;

# def vm(args, vm=None, nonZeroIsFatal=True, out=None, err=None, cwd=None, timeout=None, vmbuild=None):

def mx_init():
  commands = {
      'frtest': [frtest, ''],
      'r': [rconsoleServer, ''],
      'rg': [rconsoleGraal, ''],
      'rgd': [rconsoleDebugGraal, ''],      
      'rfannkuch': [rfannkuchServer, '[size]'],
      'rgfannkuch': [rfannkuchGraal, '[size]'],
      'rbinarytrees': [rbinarytreesServer, '[size]'],
      'rspectralnorm': [rspectralnormServer, '[size]'],      
      'rnbody': [rnbodyServer, '[size]'],
      'rfasta': [rfastaServer, '[size]'],
      'rfastaredux': [rfastareduxServer, '[size]'],      
      'runittest': [runittestServer, ''],
      'rgunittest': [runittestGraal, ''],
      'rbenchmark': [rallbenchmarksServer, ''],
      'rgbenchmark': [rallbenchmarks, '']
  }
  mx.commands.update(commands);

  # load the graal VM commands (the module that invoked us)
  gcommands = join(os.getcwd(), "mx","commands");
  mod = sys.modules.get(gcommands);

  if not hasattr(mod, 'mx_init'):
    mx.abort(gcommands + ' must define an mx_init(env) function - executing from Graal directory?');

  if not hasattr(mod, 'vm'):
    mx.abort(gcommands + ' does not have a vm command - executing from Graal directory?');

  global gmod;
  gmod = mod;

# ------------------  

def frtest(args):
  """Test FastR MX target"""
  mx.log("FastR classpath:");
  mx.log("mx.classpath(fastr) is "+mx.classpath("fastr"));
  mx.log("Server VM:");
  gmod.vm( ['-version'], vm = 'server' ); 
  mx.log("Graal VM:");
  gmod.vm( ['-XX:-BootstrapGraal', '-version'], vm = 'graal' ); 
  
def rconsoleServer(args):
  """Run R Console with the HotSpot server VM"""  
  rconsole([], 'server', args)
  
def rconsoleGraal(args):
  """Run R Console with the Graal VM"""  
  rconsole(['-XX:-BootstrapGraal'], 'graal', args)

def rconsoleDebugGraal(args):
  """Run R Console with the Graal VM, debugging options"""  
  rconsole(['-XX:-BootstrapGraal', '-G:+DumpOnError', '-G:Dump=Truffle', '-G:+PrintBinaryGraphs', '-G:+PrintCFG', '-esa'], 'graal', args)

def rfannkuchServer(args):
  """Run Fannkuch with the HotSpot server VM"""  
  rfannkuch(args, [], 'server')

def rfannkuchGraal(args):
  """Run Fannkuch with the Graal VM"""  
  rfannkuch(args, ['-XX:-BootstrapGraal'], 'graal')

def rbinarytreesServer(args):
  """Run Binary Trees with the HotSpot server VM"""  
  rbinarytrees(args, [], 'server')

def rspectralnormServer(args):
  """Run Spectral Norm with the HotSpot server VM"""  
  rspectralnorm(args, [], 'server')

def rnbodyServer(args):
  """Run NBody with the HotSpot server VM"""  
  rnbody(args, [], 'server')

def rfastaServer(args):
  """Run Fasta with the HotSpot server VM"""  
  rfasta(args, [], 'server')

def rfastareduxServer(args):
  """Run Fastaredux with the HotSpot server VM"""  
  rfastaredux(args, [], 'server')

def runittestServer(args):
  """Run unit tests with the HotSpot server VM"""
  runittest(args, [], 'server')

def rallbenchmarksServer(args):
	"""Run all benchmarks with the HotSpot server graal"""
	rbenchmarks(args, [], 'server')

def rallbenchmarks(args):
	"""Run all benchmarks on graal"""
	rbenchmarks(args, ['-XX:-BootstrapGraal'], 'graal')

def rbenchmarks(args, vmArgs, vm):
	rfannkuch(args, vmArgs, vm)
	rbinarytrees(args, vmArgs, vm)
	rspectralnorm(args, vmArgs, vm)
	rspectralnorm(args, vmArgs, vm)
	rnbody(args, vmArgs, vm)
	rfasta(args, vmArgs, vm)

def runittestGraal(args):
  """Run unit tests with the Graal VM"""
  runittest(args, ['-XX:-BootstrapGraal'], 'graal')

# ------------------

def rfannkuch(args, vmArgs, vm):
  """Run Fannkuch benchmark using the given VM"""
  rshootout(args, vmArgs, vm, "fannkuch", "fannkuchredux.r", "10L");

def rbinarytrees(args, vmArgs, vm):
  """Run Binary Trees benchmark using the given VM"""
  rshootout(args, vmArgs, vm, "binarytrees", "binarytrees.r", "15L");

def rspectralnorm(args, vmArgs, vm):
  """Run Spectral Norm benchmark using the given VM"""
  rshootout(args, vmArgs, vm, "spectralnorm", "spectralnorm.r", "800L");

def rspectralnorm(args, vmArgs, vm):
  """Run Spectral Norm benchmark using the given VM"""
  rshootout(args, vmArgs, vm, "spectralnorm", "spectralnorm.r", "800L");

def rnbody(args, vmArgs, vm):
  """Run NBody benchmark using the given VM"""
  rshootout(args, vmArgs, vm, "nbody", "nbody.r", "1000L");

def rfasta(args, vmArgs, vm):
  """Run Fasta benchmark using the given VM"""
  rshootout(args, vmArgs, vm, "fasta", "fasta.r", "10000L");

def rfastaredux(args, vmArgs, vm):
  """Run Fastaredux benchmark using the given VM"""
  rshootout(args, vmArgs, vm, "fastaredux", "fastaredux.r", "10000L");

# ------------------
  
def rconsole(vmArgs, vm, cArgs):
  """Run R Console with the given VM"""
  gmod.vm( vmArgs + ['-cp', mx.classpath("fastr") , 'r.Console' ] + cArgs, vm = vm ); 

def rshootout(args, vmArgs, vm, benchDir, benchFile, defaultArg):
  """Run given shootout benchmark using given VM"""
  if (len(args)==0):
    arg = defaultArg
  else:
    arg = args[0]

  source = join(os.getcwd(), "..", "fastr", "test", "r", "shootout", benchDir, benchFile)
  tmp = ".tmp." + benchDir + ".torun.r"

  shutil.copyfile(source, tmp)
  with open(tmp, "a") as f:
    f.write("run(" + arg + ")\n")

  if mx._opts.verbose:
	print("Input file " + tmp + ": " + arg);
  
#  rconsole(vmArgs + ['-XX:-Inline'], vm, ['--waitForKey','-f',tmp]);
#  rconsole(vmArgs, vm, ['--waitForKey', '-f', tmp]);
  rconsole(vmArgs, vm, ['-f', tmp]);

def runittest(args, vmArgs, vm): 
  """Run unit tests using the given VM""" 
  
  classes = gmod._find_classes_with_annotations(mx.project('fastr'), None, ['@Test', '@Parameters'])
  gmod.vm( ['-esa', '-ea', '-cp', mx.classpath('fastr')] + vmArgs + ['org.junit.runner.JUnitCore'] + classes, vm )
  
