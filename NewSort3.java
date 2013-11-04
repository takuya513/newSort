package newSort;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import newSort.NewSort3.SortBlock;

import quickSort.QuickSort;
import tools.MyArrayUtil;
import tools.TestTools;
/*
 * 最初のクイックソートの一部を別のスレッドに任せようとしたもの
 * しかし、なぜかバグがでてしまう
 */
public class NewSort3 <E extends Comparable> extends QuickSort<E>{
	ExecutorService executor;
	int threadsNum,arrayLength,pos,first;
	List<Callable<Object>> workers;
	ArrayList<SortBlock> works;
	ArrayList<SortBlock> works2;
	boolean endArray = false;
	boolean firstSet = false;
	int[] posInfo;
	public BlockingQueue<SortBlock> queue;
	TestTools tt;


	public NewSort3() {
		tt = new TestTools();
		threadsNum = Runtime.getRuntime().availableProcessors()-1;
		queue = new LinkedBlockingQueue<SortBlock>();
	}


	public void sort(E[] array){
		this.array = array;
		arrayLength = array.length;
		executor = Executors.newFixedThreadPool(threadsNum);
		workers = new ArrayList<Callable<Object>>(threadsNum);
		works = new ArrayList<SortBlock>();

		pos = 0;

		QuickSortWorker2 thread = new QuickSortWorker2(queue);
		thread.start();

		while(true){
			if(array[pos].compareTo(array[pos+1]) <= 0)
				ascendingOrder();
			else
				descendindOrder();

			pos++;
			if(endArray == true)
				break;
			else if(pos+1 > arrayLength-1){
				works.add(new SortBlock(pos,pos));
				break;
			}
		}

		thread.isKeepPut();
		//修正、適宜実行できるようにする
		try {

			executor.invokeAll(workers);
			thread.join();

			//マージするメソッド
			parallelMergeSort();

			executor.invokeAll(workers);
		}catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
		executor.shutdown();

//		System.out.println("testCount : "+testCount);
//		System.out.println("testCOunt2 : "+testCount2);

	}

	//昇順メソッド
	public void ascendingOrder(){
		first = pos;
		while(array[pos].compareTo(array[pos+1]) <= 0){
			if(pos+1 >= arrayLength-1){
				works.add(new SortBlock(first,pos+1));
				endArray = true;
				return;
			}
			pos++;
		}
		works.add(new SortBlock(first,pos));
		return;
	}

	//降順メソッド
	public void descendindOrder(){
			first = pos;

		while(array[pos].compareTo(array[pos+1]) > 0){
			if(pos+1 >= arrayLength-1){
				workers.add(Executors.callable(new QuickSortWorker(first,pos+1)));
				works.add(new SortBlock(first,pos+1));
				endArray = true;
				return;
			}
			pos++;
		}

		if((pos - first) < 10){   //更新
			try {
				queue.put(new SortBlock(first,pos));
			} catch (InterruptedException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}
		}else
			workers.add(Executors.callable(new QuickSortWorker(first,pos)));//修正、もっと効率よくしたい

		works.add(new SortBlock(first,pos));
		return;
	}

	//マージする部分
	public void parallelMergeSort(){
		int workSize;
		SortBlock block1,block2;
		while(true){
			workSize = works.size();

			while(workSize > 1){
				//二つのworksを取り出し、left,rightを取り出す
				block1 = works.remove(0);
				block2 = works.remove(0);

				if((block2.right - block1.left) < 10){
					merge(block1.left,block1.right,block2.right,new Object[block2.right - block1.left + 1]);
				}else
					workers.add(Executors.callable(new MergeSortWorker(block1.left,block1.right,block2.right)));

				works.add(new SortBlock(block1.left,block2.right));

				workSize = workSize-2;
				//一つ余りが出たら後ろに回す
				if(workSize == 1){
					SortBlock tmp = works.remove(0);
					works.add(tmp);
					break;
				}
			}


			try {
				executor.invokeAll(workers);
			} catch (InterruptedException e) {
				// TODO 自動生成された catch ブロック
				e.printStackTrace();
			}

			if(works.size() == 2){  //範囲の情報が二つのときは最後にマージして終了
				block1 = works.remove(0);
				block2 = works.remove(0);
				merge(block1.left,block1.right,block2.right,new Object[arrayLength]);
				break;
			}else if(works.size() == 1)  //範囲の情報が一つのときは完成しているので終了
				break;
		}
	}




	@SuppressWarnings("unchecked")
	public synchronized void merge(int left,int mid,int right,Object[] buff){
		int i = left,j = mid + 1,k = 0;
		//Object[] buff = buff2;

		while(i <= mid && j <= right) {
			if(array[i].compareTo(array[j]) < 0)
				buff[k++] = array[i++];
			else
				buff[k++] = array[j++];
		}

		while(i <= mid)
			buff[k++] = array[i++];
		while(j <= right)
			buff[k++] = array[j++];
		for(i = left;i <= right; i++)
			array[i] = (E) buff[i - left];
	}



	class MergeSortWorker implements Runnable{
		int left,right,mid;
		Object[] buff;
		public MergeSortWorker(int left,int right){
			this.left = left;
			this.right = right;
			mid = (left + right) / 2;
			buff = new Object[right - left + 1];
		}

		public MergeSortWorker(int left,int mid,int right){
			this.left = left;
			this.right = right;
			this.mid = mid;
			buff = new Object[right - left + 1];
		}
		public void run(){
			merge(left,mid,right,buff);
		}
	}


	class SortBlock{
		private int left,right;
		SortBlock(int left,int right){
			this.left = left;
			this.right = right;
		}

	}


	class QuickSortWorker implements Runnable {
		int left,right;
		public QuickSortWorker(int left,int right){
			this.left = left;
			this.right = right;
		}

		public void run() {
			quickSort(left,right);
		}
	}

	class QuickSortWorker2 extends Thread{
		public BlockingQueue<SortBlock> queue;
		private boolean keepPut = true;   //仕事の配給はつづいているか
		SortBlock sortBlock;
		public QuickSortWorker2(BlockingQueue<SortBlock> queue){
			this.queue = queue;
		}

		public void isKeepPut(){
			keepPut = false;
		}

		public void run(){

			while(keepPut || ( queue.size() > 0)){

				//int a = 0;
				//System.out.println();
				try {

					sortBlock = queue.take();
					//tt.p("bok",sortBlock.left,"ddd",sortBlock.right);
					quickSort(sortBlock.left,sortBlock.right);
				} catch (InterruptedException e) {
					// TODO 自動生成された catch ブロック
					e.printStackTrace();
				}



			}
			//tt.p("finish");
			//tt.p("queue size",queue.size() );
		}

	}


//	class Channel {
//		private final BlokingList queue;
//
//		public Channel(){
//			queue = new ArrayList();
//		}
//
//		public void putRequest(QuickInfo info){
//			queue.add(info);
//		}
//	}
}
