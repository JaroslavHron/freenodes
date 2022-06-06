/**
 * Author: Jaroslav Hron <jaroslav.hron@mff.cuni.cz>
 * Date: May 28, 2015
 * Version: 1.5
 * License: use freely for any purpose
 * Copyright: none
 * Repository: https://github.com/JaroslavHron/freenodes.git
 * Web: http://cluster.karlin.mff.cuni.cz/freenodes
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
enum Color : int {
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
  int idx;
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
  bool[string] parts;
  bool up;
}

struct Part { 
  string name;
  int idx;
  Color color;
  bool[string] feature;
  Duration max_time;
  Duration def_time;
  string[] nodes;
  int priority;
  int cores;
  Job[] jobs;
  Job[] running;
  Job[] pending;
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
  int nodes;
  int ntasks;
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

  auto pat1=regex(r"([a-z]*)([^ ]*)");
  auto nn=matchFirst(hosts, pat1).captures[1];
  auto ls=matchFirst(hosts, pat1).captures[2];
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
            nh~= nn~to!string(i);
        }
        else if(m.length==1) {
          nh~= nn~m[0];
        }
      }
  }
  else {
    nh~= nn~ls;    
  }
 
  return(nh);
}

Duration parse_time_interval(string t)
{
  // parse interval in the form  [days-]hh:mm:ss

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


auto scontrol_parts_info()
{
  auto cmd=format("scontrol -a -o -d show part");
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

  foreach(int i, string l; output)
    {
      auto p=Part();
      p.name=matchFirst(l, regex(r"(PartitionName)=([^ ]*)")).captures[2];
      p.max_time=parse_time_interval(matchFirst(l, regex(r" (MaxTime)=([^ ]*)")).captures[2]); 
      p.def_time=parse_time_interval(matchFirst(l, regex(r" (DefaultTime)=([^ ]*)")).captures[2]);
      p.priority=matchFirst(l, regex(r" (Priority|PriorityJobFactor)=([^ ]*)")).captures[2].to!int;
      p.cores=matchFirst(l, regex(r" (TotalCPUs)=([^ ]*)")).captures[2].to!int;

      auto nl=matchFirst(l, regex(r" (Nodes)=([^ ]*)")).captures[2];
      auto nlex=scontrol_expand_hosts(nl);

      p.nodes=nlex;
      p.color=Color.none;

      parts[p.name]=p;
    }
  return(parts);
}

auto scontrol_jobs_info()
{
  auto cmd=format("scontrol -a -o -d show job");
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
      
      auto nodes=matchFirst(l, regex(r" (NumNodes)=([^ ]*)")).captures[2];
      j.nodes=matchFirst(nodes, regex(r"([0-9]*)")).captures[1].to!int;

      j.ncpus=matchFirst(l, regex(r" (NumCPUs)=([^ ]*)")).captures[2].to!int;
      j.ntasks=matchFirst(l, regex(r" (NumTasks)=([^ ]*)")).captures[2].to!int;
      j.cpus_per_task=2; //matchFirst(l, regex(r" (CPUs/Task)=([^ ]*)")).captures[2].to!int;
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
  auto output=result.output.strip().split("\n");
  if (result.status != 0) {
    writeln("Failed to call scontrol utility.\n" ~ result.output);
    output.length=0;
  }
  
  Node[string] nodes;

  auto idx=0;
  foreach(int i, string l; output)
    {
      auto n=Node();
      idx++;
      n.name=matchFirst(l, regex(r"(NodeName)=([^ ]*)")).captures[2];
      //auto idx=matchFirst(n.name, regex(r"r([0-9]*)")).captures[1].to!int;
      //if (matchFirst(n.name, regex(r"d([0-9]*)")).captures[1].length>0)
      //	 {idx=10*matchFirst(n.name, regex(r"d([0-9]*)")).captures[1].to!int;}
      n.idx=idx;
      n.sockets=matchFirst(l, regex(r"(Sockets)=([^ ]*)")).captures[2].to!int;
      n.cores_per_socket=matchFirst(l, regex(r"(CoresPerSocket)=([^ ]*)")).captures[2].to!int;
      n.threads_per_core=matchFirst(l, regex(r"(ThreadsPerCore)=([^ ]*)")).captures[2].to!int;
      n.cpus=matchFirst(l, regex(r"(CPUTot)=([^ ]*)")).captures[2].to!int;
      n.mem=matchFirst(l, regex(r"(RealMemory)=([^ ]*)")).captures[2].to!int;
      n.mem_alloc=matchFirst(l, regex(r"(AllocMem)=([^ ]*)")).captures[2].to!int;
      n.hd_size=matchFirst(l, regex(r"(TmpDisk)=([^ ]*)")).captures[2].to!int;
      n.cpu_alloc=matchFirst(l, regex(r"(CPUAlloc)=([^ ]*)")).captures[2].to!int;
      n.features=matchFirst(l, regex(r"(Features)=([^ ]*)")).captures[2].strip();
      foreach(string f ; n.features.split(",")) n.feature[f]=true;
      n.sload=matchFirst(l, regex(r"(CPULoad)=([^ ]*)")).captures[2].strip();
      try n.load=n.sload.to!float; catch (ConvException) n.load=-1.0;
      n.state=matchFirst(l, regex(r"(State)=([^ ]*)")).captures[2];
      n.cores=n.sockets*n.cores_per_socket;
      auto os=matchFirst(l, regex(r"(OS)=([^=]*) RealMemory=")).captures[2];
      if( os.empty ) n.os="unkown";
      else {
          auto osid=matchFirst(os, regex(r"\#([0-9]*)")).captures[1].to!int;
          n.os="u16";
          if (osid>78) n.os="u18";
          }
      nodes[n.name]=n;
    }
  
  return(nodes);
}


bool display_user=true;
bool display_time=true;
bool display_id=false;
bool display_node=false;
bool display_running=false;
bool display_pending=false;

//string ids=".x#!!!!!!";
//wchar[] ids=['_','\u25FC','#','!','!','!','!','!','!'];
wchar[] ids=['.','x','#','!','!','!','!','!','!'];

Color[string] part_color;
string[string] status_name;


auto create_core_map(Node node, Job job)
{
  auto sum=0;
  char[] rmap;
  int[] map;
  wchar[] smap;
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
  part_color=[
              "express":Color.fgRed,
              "express3":Color.fgRed,
              "short":Color.fgBlue,
              "long":Color.fgGreen,
              "parallel":Color.fgCyan,
              "core36":Color.fgMagenta,
              "free":Color.none,
              "debug":Color.fgYellow,
              "test":Color.fgYellow,
              "other":Color.none ];

  status_name=[ "ALLOCATED":"full", "IDLE":"free", "MIXED":"part", "IDLE+COMPLETING":"wait", "MIXED+COMPLETING":"wait", "ALLOCATED+COMPLETING":"wait", "DOWN*":"down", "DOWN":"down", "DRAINED":"closed", "DRAINING":"closing", "IDLE+DRAIN":"closed", "MIXED+DRAIN":"closed" , "ALLOCATED+DRAIN":"closed" ];

  auto helpInformation = getopt(args, std.getopt.config.passThrough, std.getopt.config.bundling,
                                "id|i", "Display the job id", &display_id,
                                "node|n", "Display the node details", &display_node,
                                "time|t", "Display the remaining time of the job allocation", &display_time,
                                "user|u", "Display the user names", &display_user,
                                "running|r", "Display the list of running jobs", &display_running,
                                "pending|p", "Display the list of pending jobs", &display_pending);

  if (helpInformation.helpWanted)
    {
      defaultGetoptPrinter("List cluster occupation info from SLURM.\nSee http://cluster.karlin.mff.cuni.cz/freenodes for details.",
                           helpInformation.options);
      return;
    }

  auto head="";
  if(display_user||display_time||display_id) {
    head=" [";
    if(display_id)   head~="ID|";
    if(display_user) head~="owner|";
    if(display_time) head~="remaining time|";
    head~="cores]";
  }

  auto allnodes=scontrol_nodes_info();
  auto alljobs=scontrol_jobs_info();
  auto allparts=scontrol_parts_info();
  
  //writeln(allnodes);
  //writeln(alljobs);
  //writeln(allparts);

  foreach ( i, n ; allnodes) 
    {
      auto state= status_name.get(n.state,"unknown");
      allnodes[i].up=true;
      if (state=="down") allnodes[i].up=false;
    }

  auto idx=1;
  foreach ( i, p ; allparts ) { 
  if(!(p.name in part_color)) part_color[p.name]=Color.fgCyan;
    //p.color = part_color[p.name]; !!!!! this doesnt work
    allparts[i].color = part_color[p.name];  ///while this is OK
    allparts[i].idx = idx;
    idx+=1;
    foreach ( n ; p.nodes) {
      if(n in allnodes) { 
       allnodes[n].parts[p.name]=true ;
       }
    }
  }

  //writeln(allnodes, alljobs);
  foreach ( j ; alljobs) 
    {
      foreach( k ; j.allocations ) {
         //writeln(k,j,j.allocations);
         allnodes[k].jobs ~= j ;
	}
      allparts[j.partition].jobs ~= j ;
      if(j.state=="RUNNING") allparts[j.partition].running ~= j;
      else if(j.state=="PENDING") allparts[j.partition].pending ~= j;
    }

  bool print_mark=false;

  auto mhead=" node↔";
  if (display_node) mhead ~=" OS mem HD";
  mhead ~= " busy cores state";
  if (display_node) mhead ~=" load";
  mhead ~= " alloc cores in: ";
  writef(mhead);

  foreach( p ; allparts) writef("%1d%s ".color(p.color),p.idx,p.name);
  writeln("partition");

  int sum_cores=0;

  //auto nodes=executeShell("nodeattr -s ubuntu-14.04");
  //enforce(nodes.status == 0 , "Failed to call nodeattr utility.",nodes.output);
  //auto node_array=nodes.output.split();

  Node[] node_array;
  node_array.length=allnodes.length;

  auto i=0;
  foreach( nn ; allnodes) {
    node_array[i]=nn;
    i++;
  }

  node_array.sort!((a,b) {return a.idx < b.idx;});

  //writeln(node_array);

  foreach ( nn ; node_array)
    {

      auto node=nn; //allnodes[nn.name];

      string mark=" ";
      if (node.load>0.2 && node.state=="IDLE") mark="!";
      if (node.load>node.threads_per_core*node.cores+0.2) mark="!";
      if (mark!=" ") print_mark=true;

      auto net="↔";
      if ("InfiniBand" in node.feature) net="⇌";
      if ("InfiniBand100" in node.feature) net="⇄";
      auto hd_size="o";
      if (node.hd_size > 0 ) hd_size=".";
      if (node.hd_size > 100 ) hd_size=":";
      
      //writef("%1s%4s%1s (%2d of %2d) %6s %4s ",mark, node.name, net, node.cpu_alloc/node.threads_per_core, node.cores, status_name.get(node.state,"----"),load);
      writef("%1s%12s%1s",mark, node.name, net);
      if (display_node) writef(" %3s %3d %3d", node.os, node.mem, node.hd_size);
      writef(" (%3d of %3d) %5s ", node.cpu_alloc/node.threads_per_core, node.cores, status_name.get(node.state,"----"));
      if (display_node) writef(" % 3.0f ",node.load);

      auto n=allparts.length;
      foreach( p ; allparts) {
        if(node.parts.get(p.name,false)) {
          //writef("█".color(p.color));
          writef("%1d".color(p.color),p.idx);
          //writef("\u25FC".color(p.color));
          //writef("|".color(p.color));
        n-=1;
        }
      }
      writef(" ".replicate(n-2));

      //string[string] online=["ALLOCATED":"full", "IDLE":"free", "MIXED":"part", "IDLE+COMPLETING":"wait"];        
      //if (node.state in online ) sum_cores += node.cores;
      sum_cores += node.cores;

      //writef(node.state);
      
      auto sum=0;
      int[] map;
      wchar[] smap;
      Color[] cmap;

      map.length=node.cores;
      smap.length=node.cores;
      cmap.length=node.cores;

      //writeln("x",node,node.jobs,"x");

      writef(" |");

      for(auto k=0; k<node.cores; k++) {
        map[k]=0;
        smap[k]='-';
        cmap[k]=part_color["other"];
      }
      if(node.up) for(auto k=0; k<node.cores; k++) {map[k]=0; smap[k]=ids[0]; cmap[k]=part_color["free"];}

      foreach ( j; node.jobs)
        {
          auto job=j; //alljobs[j];
          //writeln("x",job,"x");
          if(job.state=="RUNNING") {
            for(auto k=0; k<job.cpus[node.name].length ; k+=2) {
              auto cpuid=to!int(job.cpus[node.name][k]/2);
              //if(cpuid>=node.cores) cpuid-=node.cores;
              //writeln(">",k,cpuid,node.cores);
              map[cpuid] +=1 ;
              if(node.up) {
                smap[cpuid] = ids[map[cpuid]];
                cmap[cpuid]=part_color.get(job.partition,part_color["other"]);
              } else {
                smap[cpuid] = ids[0];
                cmap[cpuid]=part_color.get(job.partition,part_color["other"]);
              }
              sum+=1;
            }
          }
        }

      for( auto k=0; k<node.cores; k++) 
        {
          writef("%s".color(cmap[k]),smap[k]);
        }

      writef("|");
      writef(" ");

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
              writef("%d".color(part_color.get(job.partition,part_color["other"])),job.cpus[node.name].length/2);
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

  writeln("partition  allocation duration   cores  jobs running     [ queue  % ]       jobs in queue    next job to go in hh:mm");
  foreach ( p ; allparts ) { 
    writef("%1d%12s ".color(p.color),p.idx,p.name);
    if (p.def_time.isNegative()) {
      writef("%-22s ", p.max_time.to!string);
    }
    else {
      writef("%-12s (max %3dh)", p.def_time.to!string, p.max_time.total!"hours");
    }
    writef(" %4d", p.cores/2);

    //p.pending.sort!("a.start_time < b.start_time");
    p.pending.sort!("a.priority > b.priority");

    auto sum=0;
    foreach( j ; p.running) {
      sum += j.tasks/2;
      //writef("( %d %d)\n",j.ncpus, j.tasks);
    }
    writef("   %3d (%3d cores)", p.running.length, sum);
    writef(" %s",  percent_bar(p.cores/2,sum,10) );
    sum_rjobs += p.running.length;
    sum_rcores += sum;

    sum=0;
    foreach( j ; p.pending) sum += j.ncpus;
    writef("  %3d (%3d cores)  ", p.pending.length, sum);
    //writef(" %s",  percent_bar(p.cores/2,sum,10) );
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

  string line=format(" %8s  %-20s   %4d   %3d (%3d cores) %s  %3d (%3d cores)".color(Color.bgBlack).color(Color.fgWhite),"TOTAL","", sum_cores, sum_rjobs, sum_rcores, percent_bar(sum_cores,sum_rcores,10), sum_pjobs, sum_pcores);
  writeln(line);
}

