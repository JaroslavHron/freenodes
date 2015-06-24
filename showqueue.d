/**
 * Author: Jaroslav Hron <jaroslav.hron@mff.cuni.cz>
 * Date: May 28, 2015
 * Version: 1.2
 * License: use freely for any purpose
 * Copyright: none
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
//import core.time;

// color support
enum Color {
  none =0,
  fgBlack = 30, fgRed, fgGreen, fgYellow, fgBlue, fgMagenta, fgCyan, fgWhite,
  bgBlack = 40, bgRed, bgGreen, bgYellow, bgBlue, bgMagenta, bgCyan, bgWhite
}

string color(string text, Color c) {
  if(c!=Color.none) return "\033[" ~ c.to!int.to!string ~ "m" ~ text ~ "\033[0m";
  else return text;
}

struct Node { 
  string name;
  int sockets; 
  int cores_per_socket;
  int threads_per_core;
  int cpus;
  int cpu_alloc;
  int cores;
  int mem;
  int mem_alloc;
  string features; 
  bool[string] feature; 
  string state;
  string sload;
  float load;
  int[] jobs;
}

struct Job { 
  int id;
  string name;
  string partition;
  string user;
  DateTime submit_time;
  DateTime start_time;
  DateTime end_time;
  Duration run_time;
  Duration time_limit;
  Duration time;
  string state;
  string reason;
  int priority;
  string nodes;
  int tasks;
  int cpus_per_task;
  int ncpus;
  int[][string] cpus;
  int[string] mem;
  string[] allocations;
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

auto scontrol_count_cpuids(string cpuids)
{
  int np=0;
  auto result=cpuids.strip().split(",");
  foreach(r; result) 
    {
      auto m=r.split("-");
      assert(m.length==1||m.length==2);
      if(m.length==2) np+=1+to!int(m[1])-to!int(m[0]); 
      if(m.length==1) np+=1;
    }
  return(np);
}
//Nodes=r[23,25-27] or Nodes=r21
auto scontrol_expand_hosts(string hosts)
{
  string[] nh;

  auto pat1=regex(r"r([^ ]*)");
  auto ls=matchFirst(hosts, pat1).captures[1];
  auto pat2=regex(r"\[([0-9,-]*)\]");
  auto ms=matchFirst(ls.strip(), pat2);
  if (ms) {
    auto result=ms.captures[1].strip().split(",");
    foreach(r; result) 
      {
        auto m=r.split("-");
        assert(m.length==1||m.length==2);
        if(m.length==2) {
          for(auto i=to!int(m[0]); i<=to!int(m[1]); i++)
            nh~= "r"~to!string(i);
        }
        else if(m.length==1) {
          nh~= "r"~m[0];
        }
      }
  }
  else {
    nh~= "r"~ls;    
  }
 
  return(nh);
}

Duration parse_time_interval(string t)
{
  // parse interval in the form  [days-]hh:mm:ss
  
  auto s=t.split("-");

  int ndays=0;
  string time;
  
  if(s.length>1) {ndays=to!int(s[0]); time=s[1];}
  else {time=s[0];}

  auto m=time.split(":");

  auto dur=days(ndays)+hours(to!int(m[0]))+minutes(to!int(m[1]))+seconds(to!int(m[2]));
  return(dur);
}

auto scontrol_jobs_info()
{
  auto cmd=format("scontrol -a -o -d show job");
  auto result=executeShell(cmd);
  enforce( result.status == 0 , "Failed to call scontrol utility.\n"~result.output);
  auto output=result.output.strip().split("\n");

  Job[int] jobs;

  foreach(int i, string l; output)
    {
      auto j=Job();
      j.name=matchFirst(l, regex(r"(Name)=([^ ]*)")).captures[2];
      j.id=matchFirst(l, regex(r"(JobId)=([^ ]*)")).captures[2].to!int;
      j.priority=matchFirst(l, regex(r"(Priority)=([^ ]*)")).captures[2].to!int;
      j.state=matchFirst(l, regex(r" (JobState)=([^ ]*)")).captures[2];
      j.reason=matchFirst(l, regex(r" (Reason)=([^ ]*)")).captures[2];
      j.partition=matchFirst(l, regex(r" (Partition)=([^ ]*)")).captures[2];
      j.user=matchFirst(l, regex(r" (Account)=([^ ]*)")).captures[2];

      j.run_time=parse_time_interval(matchFirst(l, regex(r" (RunTime)=([^ ]*)")).captures[2]);
      j.time_limit=parse_time_interval(matchFirst(l, regex(r" (TimeLimit)=([^ ]*)")).captures[2]); 

      j.submit_time=DateTime.fromISOExtString(matchFirst(l, regex(r" (SubmitTime)=([^ ]*)")).captures[2]);
      try j.start_time=DateTime.fromISOExtString(matchFirst(l, regex(r" (StartTime)=([^ ]*)")).captures[2]);
      catch(TimeException) j.start_time=j.submit_time;
      if(j.state=="RUNNING") j.end_time=DateTime.fromISOExtString(matchFirst(l, regex(r" (EndTime)=([^ ]*)")).captures[2]);
      else j.end_time=j.start_time+j.time_limit;
     
      
      auto dur0=j.time_limit-j.run_time;
      //auto s = dur0.split!("days", "hours", "minutes", "seconds")();
      //j.time=format("%d-%02d:%02d:%02d",s.days,s.hours,s.minutes,s.seconds);
      j.time=dur0;
      
      j.nodes=matchFirst(l, regex(r" (NumNodes)=([^ ]*)")).captures[2];
      j.ncpus=matchFirst(l, regex(r" (NumCPUs)=([^ ]*)")).captures[2].to!int;
      j.cpus_per_task=matchFirst(l, regex(r" (CPUs/Task)=([^ ]*)")).captures[2].to!int;
      auto nl=matchFirst(l, regex(r" (NodeList)=([^ ]*)")).captures[2];
      auto nlex=scontrol_expand_hosts(nl);
      
      string[] nl2;
      int np=0;

      foreach(c; matchAll(l, regex(r" (Nodes)=([^ ]*) (CPU_IDs)=([^ ]*) (Mem)=([^ ])"))) {
        auto tmpn=scontrol_expand_hosts(c.captures[2]);
        auto tmpnp=scontrol_expand_cpuids(c.captures[4]);
        auto mem=c.captures[6].to!int;
        foreach(k;tmpn) {np+=tmpnp.length; j.cpus[k]=tmpnp; j.mem[k]=mem;}
        nl2 ~= tmpn;
      }
      j.tasks=np;
      j.allocations=nl2;

      jobs[j.id]=j;
    }
  return(jobs);
}

auto scontrol_nodes_info()
{
  auto cmd=format("scontrol -a -o -d show node");
  auto result=executeShell(cmd);
  enforce(result.status == 0 , "Failed to call scontrol utility.\n"~result.output);
  auto output=result.output.strip().split("\n");

  Node[string] nodes;

  foreach(int i, string l; output)
    {
      auto n=Node();
      n.name=matchFirst(l, regex(r"(NodeName)=([^ ]*)")).captures[2];;
      n.sockets=matchFirst(l, regex(r"(Sockets)=([^ ]*)")).captures[2].to!int;
      n.cores_per_socket=matchFirst(l, regex(r"(CoresPerSocket)=([^ ]*)")).captures[2].to!int;
      n.threads_per_core=matchFirst(l, regex(r"(ThreadsPerCore)=([^ ]*)")).captures[2].to!int;
      n.cpus=matchFirst(l, regex(r"(CPUTot)=([^ ]*)")).captures[2].to!int;
      n.mem=matchFirst(l, regex(r"(RealMemory)=([^ ]*)")).captures[2].to!int;
      n.mem_alloc=matchFirst(l, regex(r"(AllocMem)=([^ ]*)")).captures[2].to!int;
      n.cpu_alloc=matchFirst(l, regex(r"(CPUAlloc)=([^ ]*)")).captures[2].to!int;
      n.features=matchFirst(l, regex(r"(Features)=([^ ]*)")).captures[2].strip();
      foreach(string f ; n.features.split(",")) n.feature[f]=true;
      n.sload=matchFirst(l, regex(r"(CPULoad)=([^ ]*)")).captures[2];
      try n.load=n.sload.to!float; catch (ConvException) n.load=-1.0;
      n.state=matchFirst(l, regex(r"(State)=([^ ]*)")).captures[2];
      n.cores=n.sockets*n.cores_per_socket;
      nodes[n.name]=n;
    }
  return(nodes);
}


bool display_running=false;
bool display_pending=false;
bool display_cancelled=false;

string ids=".x#!!!!!!";

Color[string] part_color;
string[string] status_name;


auto create_core_map(Node node, Job job)
{
  auto sum=0;
  char[] rmap;
  int[] map;
  char[] smap;
  Color[] cmap;
  
  map.length=node.cores;
  smap.length=node.cores;
  cmap.length=node.cores;
  rmap~=format("%s|",node.name);
  
  for(auto k=0; k<node.cores; k++) {map[k]=0; smap[k]=ids[0]; cmap[k]=part_color["free"];}
  
  if(job.state=="RUNNING") {
    for(auto k=0; k<job.cpus[node.name].length ; k++) {
      auto cpuid=job.cpus[node.name][k];
      map[cpuid] +=1 ;
      smap[cpuid] = ids[map[cpuid]];
      cmap[cpuid]=part_color.get(job.partition,part_color["other"]);
      sum+=1;
    }
  }
  
  for( auto k=0; k<node.cores; k++) rmap~=format("%s".color(cmap[k]),smap[k]);
  
  rmap~=format("|");
  return(rmap);
}

void main(string[] args)
{
  part_color=[ "express":Color.fgRed, "short":Color.fgBlue, "long":Color.fgGreen, "other":Color.fgYellow, "free":Color.none, "debug":Color.fgMagenta ];

  status_name=[ "ALLOCATED":"full", "IDLE":"free", "MIXED":"part" ];

  auto helpInformation = getopt(args, std.getopt.config.passThrough, std.getopt.config.bundling,
                                "runnig|r", "Display the running jobs", &display_running,
                                "pending|p|q", "Display the pending jobs", &display_pending,
                                "cancelled|c", "Display the cancelled jobs", &display_cancelled);

  auto nodes=executeShell("nodeattr -s ubuntu-14.04");
  enforce(nodes.status == 0 , "Failed to call nodeattr utility.",nodes.output);

  auto node_array=nodes.output.split();

  if (helpInformation.helpWanted)
    {
      defaultGetoptPrinter("List cluster occupation info from SLURM.",
                           helpInformation.options);
      return;
    }

  auto head="";
  if(display_running||display_pending||display_cancelled) {
    head=" [";
    if(display_running)   head~="ID|";
    if(display_pending) head~="owner|";
    if(display_cancelled) head~="remaining time|";
    head~="cores]";
  }

  auto allnodes=scontrol_nodes_info();
  auto alljobs=scontrol_jobs_info();

  foreach ( j ; alljobs) 
    {
      foreach( k ; j.allocations ) allnodes[k].jobs ~= j.id ;
    }

  bool print_mark=false;

  writeln("   id  queue       user remaining_time priority");

  Job[] running, pending, cancelled;
  foreach (j; alljobs) {
    if(j.state=="RUNNING") running ~= j;
    if(j.state=="PENDING") pending ~= j;
    if(j.state=="CANCELLED") cancelled ~= j;
  }

  writefln("There is %d running, %d pending, %d canceled jobs",running.length, pending.length, cancelled.length);
  
  sort!("a.time < b.time")(running);
  sort!("a.priority < b.priority")(pending);
  
  if (display_running) foreach (j; running)
    {
      auto ts=j.time.split!("hours","minutes")();
      writef("%5d %8s %10s %4d:%02d",j.id,j.partition,j.user,ts.hours,ts.minutes);
      writef(" %s on ",j.state);
      write("[");
      foreach(k ; j.allocations) {
        writef("%s",create_core_map(allnodes[k], j));
      }
      writeln("]");
    }

  if (display_pending) foreach( j; pending)
    {
      writef("%5d %8s %10s %6d",j.id,j.partition,j.user,j.priority);
      auto ets=j.start_time-cast(DateTime)(Clock.currTime());
      auto ts=ets.split!("hours","minutes")();
      writefln(" %s waiting for %s (%s, %s nodes, %d cpus) - estimated start in %4d:%02d",j.state,j.reason,j.time,j.nodes,j.ncpus,ts.hours,ts.minutes);
    }

  
  if (display_cancelled) foreach( j; cancelled)
    {
      writef("%5d %8s %10s %10s %6d",j.id,j.partition,j.user,j.run_time,j.priority);
        writefln(" %s ",j.state);
    }
  writeln("");
}

