/**
 * Author: Jaroslav Hron <jaroslav.hron@mff.cuni.cz>
 * Date: May 9, 2026
 * Version: 2.0
 * License: use freely for any purpose
 * Copyright: none
 * Repository: https://github.com/JaroslavHron/freenodes.git
 **/

/**
 * Notes: needs a lot of code cleanup....
 **/

import std.stdio;
import std.string;
import std.process;
import std.array;
import std.conv;
import std.getopt;
import std.regex;
import std.exception;
import std.algorithm;
import std.datetime;
import std.typecons : tuple;

// color support
enum Color : int {
  none =0,
  fgBlack = 30, fgRed, fgGreen, fgYellow, fgBlue, fgMagenta, fgCyan, fgWhite,
  bgBlack = 40, bgRed, bgGreen, bgYellow, bgBlue, bgMagenta, bgCyan, bgWhite
}

string color(string text, Color c) {
  if(c!=Color.none) return "\033[" ~ c.to!int.to!string ~ "m" ~ text ~ "\033[0m";
  else return text;
}

struct Cluster {
  uint idx;
  string name;
}
  
struct Node { 
  uint idx;
  string name;
  string[string] info;
  int sockets; 
  int cores_per_socket;
  int threads_per_core;
  int cpus;
  int cpu_alloc;
  int cores;
  int hd_size;
  string os;
  int mem;
  int mem_alloc;
  int disk_total;
  int disk_free;
  string features; 
  bool[string] feature; 
  string state;
  string sload;
  float load;
  Job[] jobs;
  string[] parts;
  bool up;
}

struct Part { 
  uint idx;
  string name;
  string[string] info;
  char label;
  Color color;
  bool[string] feature;
  Duration max_time;
  Duration def_time;
  string[] nodes;
  ulong priority;
  int cpus;
  int cores;
  Job[] jobs;
  Job[] running;
  Job[] pending;
}

struct Job { 
  uint idx;
  string name;
  int id;
  string[string] info;
  string partition;
  string account;
  string user;
  DateTime submit_time;
  DateTime start_time;
  DateTime end_time;
  Duration run_time;
  Duration time_limit;
  Duration time;
  string state;
  string reason;
  ulong priority;
  int nodes;
  string[] node_list;
  int tasks;
  int ncpus;
  int[][string] cpus;
  int[string] cores;
  int[string] mem;
}


import std.string : toStringz, fromStringz;

extern (C) {
  // We treat bitstr_t as an opaque struct in D
  struct bitstr_t;

  // Core bitstring functions from Slurm
  bitstr_t* slurm_bit_alloc(int nbits);
  void slurm_bit_free(bitstr_t* b);
  int slurm_bit_unfmt(bitstr_t* b, const(char)* str);
  int slurm_bit_ffs(bitstr_t* b);
  int slurm_bit_size(bitstr_t* b);
  int slurm_bit_test(bitstr_t* b, int bit);
}

// not working
void slurm_expand_cpuids(string cpuids, int ncpus) {

  bitstr_t* myBitmap = slurm_bit_alloc(ncpus);
  if (!myBitmap) return;
  scope(exit) slurm_bit_free(myBitmap); // Ensure memory is freed

  if (slurm_bit_unfmt(myBitmap, cpuids.toStringz) != 0) {
    writeln("Error: Could not parse CPU string.");
    return;
  }

  // Instead of bit_next_set, we iterate the whole size of the bitmap
  int size = slurm_bit_size(myBitmap);
  writeln("Expanded IDs:", size);
  foreach (i; 0 .. size) {
    if (slurm_bit_test(myBitmap, i)) {
      write(i," ");
    }
    //writeln(i, " ", slurm_bit_test(myBitmap, i));
  }
}

auto scontrol_expand_cpuids(string cpuids)
{
  int[] np;
  auto result=cpuids.strip().split(",");
  foreach(r; result) 
    {
      auto m=r.split("-");
      assert(m.length==1||m.length==2);
      if(m.length==2) {
        for(auto i=to!int(m[0]); i<=to!int(m[1]); i++)
          np~=i;
      }
      else if(m.length==1) {
        np~=to!int(m[0]);
      }
    }
  return(np);
}


// use slurm internal functions to expand ranges
// Link with -lslurm
extern(C) {
    // Represents a Slurm hostlist object
    alias hostlist_t = void*;
    // Creates a hostlist from a Slurm node string (e.g., "node[01-03,05]")
    hostlist_t slurm_hostlist_create(const char* hostlist);
    // Pops the next hostname from the list (returns NULL when empty)
    char* slurm_hostlist_shift(hostlist_t hl);
    // Frees the hostlist object memory
    void slurm_hostlist_destroy(hostlist_t hl);
    // Frees strings returned by hostlist_shift (Slurm uses xfree internally)
    void free(void* ptr); 
}

auto slurm_expand_hosts(string hosts)
{
    hostlist_t hl = slurm_hostlist_create(hosts.toStringz);
    scope(exit) slurm_hostlist_destroy(hl);

    string[] output;
    char* name;
    while ((name = slurm_hostlist_shift(hl)) != null) {
        output ~= name.fromStringz.idup;
        free(name); // Critical: shift allocates a new string each time
    }
  return(output);
}

// parse interval in the form  [days-]hh:mm:ss
Duration parse_time_interval(string t)
{

  if (t=="INVALID") return(seconds(-1));
  if (t=="NONE") return(seconds(-1));
  if (t=="UNLIMITED") return(seconds(-1));

  auto s=t.split("-");

  int ndays=0;
  string time;
  
  if(s.length>1) {ndays=to!int(s[0]); time=s[1];}
  else {time=s[0];}

  auto m=time.split(":");

  auto dur=days(ndays)+hours(to!int(m[0]))+minutes(to!int(m[1]))+seconds(to!int(m[2]));
  return(dur);
}

// get list of available clusters by `sacctmgr list clusters`
auto slurm_clusters_info()
{
  auto cmd=format("sacctmgr -p -n list clusters");
  scope(failure) {
      writeln("Failed to call sacctmgr utility: " ~ cmd);
  }

  auto result=executeShell(cmd);
  auto output=result.output.strip().split("\n");

  if (result.status != 0) {
    writeln("Failed to call scontrol utility.\n" ~ result.output);
    output.length=0;
  }

  Cluster[string] clusters;

  auto clreg = ctRegex!r"([^|]*)|";
  foreach(i, string l; output)
    {
      scope(failure) writeln("Failed to parse:" ~ l);

      auto c=Cluster();
      c.idx=cast(uint) i;
      c.name=matchFirst(l, clreg).captures[1];
      clusters[c.name]=c;
    }
  return(clusters);

}

// fill the Part structure from the `scontrol -a -o -d show part`
auto scontrol_parts_info()
{
  auto cmd=format("scontrol -a -o -d -M %s show part",active_cluster);
  scope(failure) {
      writeln("Failed to call scontrol utility: " ~ cmd);
  }
  
  auto result=executeShell(cmd);
  auto output=result.output.strip().split("\n");
  
  if (result.status != 0) {
    writeln("Failed to call scontrol utility.\n" ~ result.output);
    output.length=0;
  }

  Part[string] parts;

  foreach(i, string l; output)
    {
      scope(failure) writeln("Failed to parse:" ~ l);

      auto slurmReg = ctRegex!r"(?P<var>[^ =]+)=(?P<value>[^ ]+)";
      auto cx = matchAll(l, slurmReg)
        .map!(t => tuple(t["var"], t["value"]))
        .array;

      auto aa = assocArray(cx);
      aa.rehash;
      
      auto p=Part();
      p.name=aa["PartitionName"];
      p.info=aa;
      p.max_time=parse_time_interval(aa["MaxTime"]); 
      p.def_time=parse_time_interval(aa["DefaultTime"]);
      p.priority=aa.get("Priority", "0").to!ulong;
      p.cpus=aa["TotalCPUs"].to!int;

      auto nlex=slurm_expand_hosts(aa["Nodes"]);
      p.nodes=nlex;
      p.color=Color.none;
      
      parts[p.name]=p;
    }
  return(parts);
}

auto scontrol_jobs_info()
{
  auto cmd=format("scontrol -a -o -d -M %s show job", active_cluster);
  scope(failure) {
      writeln("Failed to call scontrol utility: " ~ cmd);
  }

  auto result=executeShell(cmd);
  auto output=result.output.strip().split("\n");
  if (result.status != 0) {
    writeln("Failed to call scontrol utility.\n" ~ result.output);
    output.length=0;
  }

  Job[int] jobs;

  if(output.length==0) return(jobs);
  if(!cmp(output[0],"No jobs in the system")) return(jobs);

  auto slurmReg = ctRegex!r"(?P<var>[^ =]+)=(?P<value>[^ ]+)";
 
  foreach(i, string l; output)
    {
      auto j=Job();
      scope(failure) {writeln("Failed to parse:" ~ l); writeln(j);}

      auto cx = matchAll(l, slurmReg)
        .map!(t => tuple(t["var"], t["value"]))
        .array;

      //writeln(cx);
      auto aa = assocArray(cx);
      aa.rehash;
      //writeln(aa);
      j.info=aa;
      
      j.name=aa["JobName"];
      j.id=aa["JobId"].to!int;
      j.priority=aa["Priority"].to!ulong;
      j.state=aa["JobState"];
      j.reason=aa["Reason"];
      j.partition=aa["Partition"];
      j.account=aa["Account"];
      j.user=aa["UserId"].split("(")[0];
  
      j.run_time=parse_time_interval(aa["RunTime"]);

      try j.time_limit=parse_time_interval(aa["TimeLimit"]); 
      catch(TimeException) j.time_limit=days(365);

      j.submit_time=DateTime.fromISOExtString(aa["SubmitTime"]);
      try j.start_time=DateTime.fromISOExtString(aa["StartTime"]);
      catch(TimeException) j.start_time=j.submit_time;
      
      j.nodes=aa["NumNodes"].split("-")[0].to!int;
      j.ncpus=aa["NumCPUs"].split("-")[0].to!int;
      int np=0;

      if(j.state=="RUNNING") {
	try j.end_time=DateTime.fromISOExtString(aa["EndTime"]);
	catch(TimeException) j.end_time=DateTime(3000, 1, 1,0,0,0);
	
	//writeln(aa["NodeList"]);
	//writeln(cx.filter!(t => t[0] == "Nodes"));
	j.node_list=slurm_expand_hosts(aa["NodeList"]);
	auto nN = cx.filter!(t => t[0] == "Nodes").array;
	auto nC = cx.filter!(t => t[0] == "CPU_IDs").array;
	auto nM = cx.filter!(t => t[0] == "Mem").array;
	
	for(auto o=0; o<nN.length; o++)  
	  {
	    auto tmpn=slurm_expand_hosts(nN[o][1]);
	    auto tmpnp=scontrol_expand_cpuids(nC[o][1]);
	    auto mem=nM[o][1].to!int;
	    foreach(k;tmpn) {
	      np+=tmpnp.length;
	      j.cpus[k]=tmpnp;
	      j.mem[k]=mem;
	    }
	  }
	//writeln(j.node_list);
	//writeln(j.cpus);

      }
      j.tasks=np;
      j.time=j.time_limit-j.run_time;
      
      jobs[j.id]=j;
    }
  return(jobs);
}

auto scontrol_nodes_info()
{
  auto cmd=format("scontrol -a -o -d -M %s show node", active_cluster);
  auto result=executeShell(cmd);
  auto output=result.output.strip().split("\n");
  if (result.status != 0) {
    writeln("Failed to call scontrol utility.\n" ~ result.output);
    output.length=0;
  }
  
  Node[string] nodes;

  auto idx=0;
  foreach(i, string l; output)
    {
      scope(failure) writeln("Failed to parse:" ~ l);
      auto n=Node();
      idx++;

      auto slurmReg = ctRegex!r"(?P<var>[^ =]+)=(?P<value>[^ ]+)";
      auto cx = matchAll(l, slurmReg)
        .map!(t => tuple(t["var"], t["value"]))
        .array;

      auto aa = assocArray(cx);
      aa.rehash;
      
      n.name=aa["NodeName"];
      n.idx=idx;
      n.info=aa;
      n.sockets=aa["Sockets"].to!int;
      n.cores_per_socket=aa["CoresPerSocket"].to!int;
      n.threads_per_core=aa["ThreadsPerCore"].to!int;
      n.cpus=aa["CPUTot"].to!int;
      n.mem=aa["RealMemory"].to!int;
      n.mem_alloc=aa["AllocMem"].to!int;
      n.hd_size=aa["TmpDisk"].to!int;
      n.cpu_alloc=aa["CPUAlloc"].to!int;
      n.features=aa["AvailableFeatures"].strip();
      foreach(string f ; n.features.split(",")) n.feature[f]=true;
      n.sload=aa["CPULoad"].strip();
      try n.load=n.sload.to!float; catch (ConvException) n.load=-1.0;
      n.state=aa["State"].split("+")[0];
      n.cores=n.sockets*n.cores_per_socket;
      n.os=aa.get("OS", "unkown");
      nodes[n.name]=n;
    }
  
  return(nodes);
}


wchar[] ids=['.','+','#','!','!','!','!','!','!'];
string charset = "0123456789" ~ "ABCDEFGHIJKLMNOPQRSTUVWXYZ" ~ "abcdefghijklmnopqrstuvwxyz" ~ "!@#$%"; 

Color[string] part_color;
string[string] status_name;

bool display_user=true;
bool display_time=false;
bool display_jobs=true;
bool display_id=false;
bool display_node=false;
bool display_running=false;
bool display_pending=false;
bool list_clusters=false;
string active_cluster="";
string partition_select="";


/* This is assumption on slurm logical to physical map on cpus (=threads)
   I don't know how to get this from slurm, it can be obtaoned on given machine by

   hwloc-ls --no-io --only pu --of console

*/

//auto coreid = (int cpuid, int ncores) => cpuid%ncores; //asuming cpuid 0,16 are on the same core
auto coreid = (int cpuid, int ncores) => cpuid/2; //asuming cpuid 0,1 are on the same core


void main(string[] args)
{
  
  status_name=[
	       "ALLOCATED":"full",
	       "IDLE":"free",
	       "MIXED":"part",
	       "DOWN":"down",
	       ];
  
 auto helpInformation = getopt(args, std.getopt.config.passThrough, std.getopt.config.bundling,
				"cluster|c", "Select the cluster", &active_cluster,
				"list_clusters|l", "List available clusters", &list_clusters,
                                "id|i", "Display the job id", &display_id,
				"jobs|j", "Display running jobs info", &display_jobs,
                                "node|n", "Display the node details", &display_node,
                                "time|t", "Display the remaining time of the job allocation", &display_time,
                                "user|u", "Display the user names", &display_user,
                                "partition|p", "Display given partition only", &partition_select,
                                "running|R", "Display the list of running jobs", &display_running,
                                "pending|P", "Display the list of pending jobs", &display_pending);

  if (helpInformation.helpWanted)
    {
      defaultGetoptPrinter("List cluster occupation info from SLURM.\nSee http://cluster.karlin.mff.cuni.cz/freenodes for details.",
                           helpInformation.options);
      return;
    }
  
  auto allclusters=slurm_clusters_info();
  if (list_clusters) {writeln(allclusters); return;}
  if ( active_cluster== "")
    active_cluster = allclusters.byValue.find!(c => c.idx == 0).front.name;
  writeln("Cluster: "~active_cluster);
  

  if(!display_jobs) {display_user=false; display_id=false; display_time=false;}
  
  auto head="";
  if(display_user||display_time||display_id) {
    head=" [";
    if(display_id)   head~="ID|";
    if(display_user) head~="owner|";
    if(display_time) head~="remaining time|";
    head~="cpus]";
  }

  auto allnodes=scontrol_nodes_info();
  auto alljobs=scontrol_jobs_info();
  auto allparts=scontrol_parts_info();
  
  //writeln(allnodes);
  //writeln(alljobs);
  //writeln(allparts);

  foreach ( ref n ; allnodes) 
    {
      auto state = status_name.get(n.state,"unknown");
      n.up = true;
      if (state=="down") n.up=false;
    }

  part_color=[
	      "edu":Color.fgYellow,
	      "rse":Color.fgYellow,
	      "debug":Color.fgYellow,
              "test":Color.fgYellow,
              "math":Color.fgBlue,
              "other":Color.none
	      ];

  auto idx=1;
  foreach ( ref p ; allparts ) {
    if(p.name.canFind("gpu")) part_color[p.name]=Color.fgMagenta;
    if(p.name.canFind("ffa")) part_color[p.name]=Color.fgRed;
    if(!(p.name in part_color)) part_color[p.name]=Color.fgCyan;
    p.color = part_color[p.name];
    p.idx = idx;
    p.label= charset[idx]; 
    idx+=1;
    p.cores=0;
    foreach ( n ; p.nodes) {
      if(n in allnodes) { 
	allnodes[n].parts ~= p.name ;
	p.cores += allnodes[n].cores;
      }
    }
  }
  
  //writeln(allnodes, alljobs);
  foreach ( ref j ; alljobs) 
    {
      foreach( k ; j.node_list ) {
	 j.cores[k] = j.cpus[k].length.to!int / allnodes[k].threads_per_core;
         allnodes[k].jobs ~= j ;
	}
      allparts[j.partition].jobs ~= j ;
      if(j.state=="RUNNING") allparts[j.partition].running ~= j;
      else if(j.state=="PENDING") allparts[j.partition].pending ~= j;
    }

  bool print_mark=false;

  auto mhead="   node name ↔";
  if (display_node) mhead ~=" OS mem HD";
  mhead ~= "   busy cores state";
  if (display_node) mhead ~=" load";
  mhead ~= " alloc cores in: ";
  writef(mhead);


  auto part_array = allparts.byValue.array.sort!((a, b) => a.idx < b.idx).array;
  if(partition_select!="")
    part_array = part_array.filter!(p => p.name==partition_select).array;

  /*
  foreach( p ; part_array) {
    writef("%1s".color(p.color),p.label);
    writef("%s ",p.name);
  }
  */  
  writeln("partitions");

  
  int sum_cores=0;

  auto node_array = allnodes.byValue.array.sort!((a, b) => a.idx < b.idx).array;
  if(partition_select!="")
    node_array = node_array.filter!(n => n.parts.canFind!(p => p==partition_select)).array;

  
  foreach ( nn ; node_array)
      {

      auto node=nn;

      //writeln(node.features);
      //writeln(node.state, "->", status_name.get(node.state,"----"));
      
      string mark=" ";
      if (node.load>0.2 && node.state=="IDLE") mark="!";
      if (node.load>node.threads_per_core*node.cores+0.2) mark="!";
      if (mark!=" ") print_mark=true;

      auto net="↔";
      if ("ib" in node.feature) net="⇄";

      writef("%1s%12s%1s",mark, node.name, net);
      if (display_node) writef(" %3s %3d %3d", node.os, node.mem, node.hd_size);
      writef(" (%3d of %3dx%1d) %5s ", node.cpu_alloc/node.threads_per_core, node.cores, node.threads_per_core, status_name.get(node.state,"----"));
      if (display_node) writef(" % 3.0f ",node.load);

      foreach( p ; node.parts) {
	auto pp = allparts[p];
	writef("%1s".color(pp.color),pp.label);
      }
      writef(" ".replicate(10-node.parts.length));

      sum_cores += node.cores;

      auto sum=0;
      int[] map;
      wchar[] smap;
      Color[] cmap;

      map.length=node.cores;
      smap.length=node.cores;
      cmap.length=node.cores;

      //writeln("xxxx cores=",node.cores," tperc=",node.threads_per_core," cpus=",node.cpus,"xxxx");

      for(auto k=0; k<node.cores; k++) {
        map[k]=0;
        smap[k]='-';
        cmap[k]=part_color["other"];
      }
      if(node.up) for(auto k=0; k<node.cores; k++) {map[k]=0; smap[k]=ids[0]; cmap[k]=part_color["other"];}

      foreach ( j; node.jobs)
        {
          auto job=j; //alljobs[j];
          //if (job.info["NumTasks"]==job.info["NumCPUs"]) writeln("x-->",job.info["NumTasks"]," ",job.info["NumCPUs"]," ",job.info["NumNodes"]);
	  //writeln("x-->",job.info["NumTasks"]," ",job.info["NumCPUs"]," ",job.info["NumNodes"]);

          //writeln("x-->",job.info,"---x");
          if(job.state=="RUNNING") {
	    //writeln("xxxx cores=",node.cores," tperc=",node.threads_per_core," cpus=",node.cpus,"xxxx");
	    //writeln(">",job.info);
	    //writeln(">",job.cpus[node.name]);
            //for(auto k=0; k<job.cpus[node.name].length ; k++) {
            //  auto cpuid=to!int(job.cpus[node.name][k]);
            foreach(k; job.cpus[node.name]) {
              auto cpuid=k ; //to!int(k);
              //if(cpuid>=node.cores) cpuid-=node.cores;
              //writeln(">",k,cpuid,node.cores);

              map[coreid(cpuid,node.cores)] +=1 ;  
              if(node.up) {
                smap[coreid(cpuid,node.cores)] = ids[map[coreid(cpuid,node.cores)]];
                cmap[coreid(cpuid,node.cores)]=part_color.get(job.partition,part_color["other"]);
              } else {
                smap[cpuid] = ids[0];
                cmap[cpuid]=part_color.get(job.partition,part_color["other"]);
              }
              sum+=1;
            }
          }
        }

      writef(" [");
      for( auto l=0; l<node.sockets; l++){
	for( auto k=0; k<node.cores_per_socket; k++) {
	  auto m=l*node.cores_per_socket+k;
	  writef("%s".color(cmap[m]),smap[m]);
	}
	if (l<node.sockets-1) writef("|");
      }
      writef("] ");

      if(display_user || display_time || display_id)
        {
          foreach ( j; node.jobs)
            {
              auto job=j; //alljobs[j];
              if(job.state=="RUNNING") {
              string id="";
              if (display_id) id~=format("%s|",job.id);
              if (display_user) id~=format("%.2s|",job.user);
              if (display_time) {
                auto ts=job.time.split!("hours","minutes")();
                id~=format("%d:%02d|",ts.hours,ts.minutes);
              }
              writef("[");
              writef("%s".color(part_color.get(job.partition,part_color["other"])),id);
              writef("%d".color(part_color.get(job.partition,part_color["other"])),j.cores[node.name]);
              writef("]");
              }
            }
        }
      writeln("");
    }
  if(print_mark) writeln("Notes: !-marked nodes are overcommited or busy with job outside the slurm control.");

  Job[] running, pending, cancelled;
  foreach (j; alljobs) {
    if(j.state=="RUNNING") running ~= j;
    if(j.state=="PENDING") pending ~= j;
    if(j.state=="CANCELLED") cancelled ~= j;
  }

  writef("There are %d running jobs", running.length);
  if (pending.length>0) writefln(" and %d queued pending jobs.", pending.length);
  else writeln(".");

  pending.sort!("a.priority > b.priority");
  //pending.sort!("a.start_time < b.start_time");

  if (display_running) foreach( j; running)
    {
      writef("%5d ",j.id);
      writef("%8s ".color(allparts[j.partition].color),j.partition);
      writef("%10s ",j.user,);
      auto ets=j.start_time-cast(DateTime)(Clock.currTime());
      auto ts=ets.split!("hours","minutes")();
      writefln(" %s [ %2d nodes, %4d cores] - remining time: %s",j.state,j.nodes,j.ncpus,j.time);
    }

  if (display_pending) foreach( j; pending ~ cancelled)
    {
      writef("%5d ",j.id);
      writef("%8s ".color(allparts[j.partition].color),j.partition);
      writef("%10s %6d",j.user,j.priority);
      auto ets=j.start_time-cast(DateTime)(Clock.currTime());
      auto ts=ets.split!("hours","minutes")();
      writefln(" %s waiting for %s (%s, %2d nodes, %4d cpus) - estimated start in %4d:%02d",j.state,j.reason,j.time,j.nodes,j.ncpus,ts.hours,ts.minutes);
    }

  string percent_bar(int total, int part, int N) {
    int x=0;
    //writefln("[%d  %d  %d]",total,part,N);
    if (part>total) part=total;
    if (part>0) x=(N*part)/total;
    string fmt = format("[%%%ds%%%ds] %%3d%%%%",x,N-x);
    //string output = format(fmt,"▌".replicate(x),"▒".replicate(N-x),x);
    int aux=0;
    if (total>0) aux=(100*part)/total;
    string output = format(fmt,"|".replicate(x),".".replicate(N-x),aux);
    return(output);
  }

  int sum_rjobs=0;
  int sum_rcores=0;
  int sum_pjobs=0;
  int sum_pcores=0;

  writeln("partition         allocation duration     cores  jobs running      [ queue  % ]       jobs in queue    next job to go in hh:mm");

  foreach ( p ; part_array ) { 
    writef("%1s".color(p.color),p.label);
    writef("%16s ",p.name);
    if (p.def_time.isNegative()) {
      writef("%-22s ", p.max_time.to!string);
    }
    else {
      writef("%-12s (max %3dh)", p.def_time.to!string, p.max_time.total!"hours");
    }
    writef(" %5d", p.cores);

    //p.pending.sort!("a.start_time < b.start_time");
    p.pending.sort!("a.priority > b.priority");

    auto sum=0;
    foreach( j ; p.running) {
      sum += j.tasks;
      //writef("( %d %d)\n",j.ncpus, j.tasks);
    }
    writef("  %4d (%4d cores)", p.running.length, sum);
    writef(" %s",  percent_bar(p.cores,sum,10) );
    sum_rjobs += p.running.length;
    sum_rcores += sum;

    sum=0;
    foreach( j ; p.pending) sum += j.ncpus;
    writef("  %3d (%4d cores)  ", p.pending.length, sum);
    //writef(" %s",  percent_bar(p.cores,sum,10) );
    sum_pjobs += p.pending.length;
    sum_pcores += sum;

    int k=0;
    foreach ( j; p.pending)
      {
        if(k<1) {
          auto job=j; //alljobs[j];
          string id="";
          if (display_id) id~=format("%s|",job.id);
          if (display_user) id~=format("%s|",job.user);
          if (display_time) {
            auto ets=job.start_time-cast(DateTime)(Clock.currTime());
            auto ts=ets.split!("hours","minutes")();
            id~=format("%d:%02d|",ts.hours,ts.minutes);
          }
          writef("[");
          writef("%s".color(part_color.get(job.partition,part_color["other"])),id);
          writef("%d".color(part_color.get(job.partition,part_color["other"])),job.ncpus);
          writef("]");
          //write("@",job.priority);
        }
        k++;
      }

    writef("\n");
  }

  string line=format(" %16s  %-20s    %4d %5d (%4d cores) %s  %3d (%4d cores)".color(Color.bgBlue).color(Color.fgWhite),"TOTAL","", sum_cores, sum_rjobs, sum_rcores, percent_bar(sum_cores,sum_rcores,10), sum_pjobs, sum_pcores);
  writeln(line);
}

